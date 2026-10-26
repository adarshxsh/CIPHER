package distribution

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/transport"
)

func TestDefaultUploaderConfig(t *testing.T) {
	if DefaultUploaderConfig.MaxConcurrentProviders != 8 {
		t.Errorf("expected DefaultUploaderConfig.MaxConcurrentProviders to be 8, got %d",
			DefaultUploaderConfig.MaxConcurrentProviders)
	}
}

func setupTestEngine(t *testing.T, contentID core.ContentID) (*engine.ContentEngine, func()) {
	tmpDir, err := os.MkdirTemp("", "uploader-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)
	if err := eng.PutManifestBytes(context.Background(), contentID, []byte("test-manifest")); err != nil {
		t.Fatalf("failed to put manifest bytes: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}
	return eng, cleanup
}

func TestDistributeConcurrencyThrottling(t *testing.T) {
	oldFunc := uploadToProviderFunc
	defer func() { uploadToProviderFunc = oldFunc }()

	var activeWorkers atomic.Int32
	var maxSeenWorkers atomic.Int32

	uploadToProviderFunc = func(
		ctx context.Context,
		tr *transport.Transport,
		eng *engine.ContentEngine,
		contentID core.ContentID,
		targetPeer peer.ID,
		chunks []core.ChunkID,
		manifestBytes []byte,
		tracker *GlobalReplicaTracker,
		maxRetries int,
	) {
		curr := activeWorkers.Add(1)
		for {
			max := maxSeenWorkers.Load()
			if curr <= max || maxSeenWorkers.CompareAndSwap(max, curr) {
				break
			}
		}

		time.Sleep(20 * time.Millisecond)

		for _, cid := range chunks {
			tracker.SetStatus(cid, targetPeer, ReplicaCommitted)
		}

		activeWorkers.Add(-1)
	}

	contentID := core.ContentID{0x01}
	chunk1 := core.ChunkID{0x10}
	chunk2 := core.ChunkID{0x20}

	plan := &PlacementPlan{
		ContentID:   contentID,
		Replication: 2,
		Manifest: &manifest.Manifest{
			Version: 1,
			Descriptor: manifest.ContentDescriptor{
				ID: contentID,
			},
			ChunkIDs: []core.ChunkID{chunk1, chunk2},
		},
		Assignments:    make(map[core.ChunkID][]peer.ID),
		ProviderChunks: make(map[peer.ID][]core.ChunkID),
	}

	// Create 20 provider peers
	providers := make([]peer.ID, 20)
	for i := 0; i < 20; i++ {
		providers[i] = peer.ID(fmt.Sprintf("provider-%d", i))
		plan.ProviderChunks[providers[i]] = []core.ChunkID{chunk1, chunk2}
	}
	plan.Assignments[chunk1] = providers
	plan.Assignments[chunk2] = providers

	tracker := NewGlobalReplicaTracker(plan.Replication)
	eng, cleanup := setupTestEngine(t, contentID)
	defer cleanup()

	cfg := UploaderConfig{
		MaxConcurrentProviders: 3,
		MaxRetriesPerChunk:     1,
		FailoverRounds:         1,
	}

	ctx := context.Background()
	if err := Distribute(ctx, nil, eng, plan, tracker, cfg); err != nil {
		t.Fatalf("Distribute failed: %v", err)
	}

	if maxSeenWorkers.Load() > 3 {
		t.Errorf("expected max concurrent workers <= 3, got %d", maxSeenWorkers.Load())
	}

	if !tracker.IsComplete(plan.Manifest.ChunkIDs) {
		t.Errorf("expected tracker to be complete")
	}
}

func TestDistributeFailoverConcurrencyThrottling(t *testing.T) {
	oldFunc := uploadToProviderFunc
	defer func() { uploadToProviderFunc = oldFunc }()

	var activeWorkers atomic.Int32
	var maxSeenPhase1Workers atomic.Int32
	var maxSeenPhase2Workers atomic.Int32
	var inFailover atomic.Bool

	uploadToProviderFunc = func(
		ctx context.Context,
		tr *transport.Transport,
		eng *engine.ContentEngine,
		contentID core.ContentID,
		targetPeer peer.ID,
		chunks []core.ChunkID,
		manifestBytes []byte,
		tracker *GlobalReplicaTracker,
		maxRetries int,
	) {
		curr := activeWorkers.Add(1)

		if inFailover.Load() {
			for {
				max := maxSeenPhase2Workers.Load()
				if curr <= max || maxSeenPhase2Workers.CompareAndSwap(max, curr) {
					break
				}
			}
		} else {
			for {
				max := maxSeenPhase1Workers.Load()
				if curr <= max || maxSeenPhase1Workers.CompareAndSwap(max, curr) {
					break
				}
			}
		}

		time.Sleep(20 * time.Millisecond)

		// Make provider-0 through provider-9 fail in Phase 1, others succeed
		if targetPeer == peer.ID("provider-0") || targetPeer == peer.ID("provider-1") ||
			targetPeer == peer.ID("provider-2") || targetPeer == peer.ID("provider-3") ||
			targetPeer == peer.ID("provider-4") || targetPeer == peer.ID("provider-5") ||
			targetPeer == peer.ID("provider-6") || targetPeer == peer.ID("provider-7") ||
			targetPeer == peer.ID("provider-8") || targetPeer == peer.ID("provider-9") {
			if !inFailover.Load() {
				for _, cid := range chunks {
					tracker.SetStatus(cid, targetPeer, ReplicaFailed)
				}
			} else {
				for _, cid := range chunks {
					tracker.SetStatus(cid, targetPeer, ReplicaCommitted)
				}
			}
		} else {
			for _, cid := range chunks {
				tracker.SetStatus(cid, targetPeer, ReplicaCommitted)
			}
		}

		activeWorkers.Add(-1)
	}

	contentID := core.ContentID{0x02}
	chunk1 := core.ChunkID{0x11}

	plan := &PlacementPlan{
		ContentID:   contentID,
		Replication: 5,
		Manifest: &manifest.Manifest{
			Version: 1,
			Descriptor: manifest.ContentDescriptor{
				ID: contentID,
			},
			ChunkIDs: []core.ChunkID{chunk1},
		},
		Assignments:    make(map[core.ChunkID][]peer.ID),
		ProviderChunks: make(map[peer.ID][]core.ChunkID),
	}

	// 15 providers in plan
	providers := make([]peer.ID, 15)
	for i := 0; i < 15; i++ {
		p := peer.ID(fmt.Sprintf("provider-%d", i))
		providers[i] = p
		plan.ProviderChunks[p] = []core.ChunkID{chunk1}
	}
	plan.Assignments[chunk1] = providers

	tracker := NewGlobalReplicaTracker(plan.Replication)
	eng, cleanup := setupTestEngine(t, contentID)
	defer cleanup()

	cfg := UploaderConfig{
		MaxConcurrentProviders: 2,
		MaxRetriesPerChunk:     1,
		FailoverRounds:         2,
	}

	// Run Distribute
	ctx := context.Background()

	// Switch inFailover flag after initial phase or inside mock function logic
	// In Phase 1, active providers in plan.ProviderChunks are 4 providers (0..3), so maxPhase1 <= 2
	// Phase 2 will run failover for missing replicas with maxPhase2 <= 2
	go func() {
		time.Sleep(30 * time.Millisecond)
		inFailover.Store(true)
	}()

	if err := Distribute(ctx, nil, eng, plan, tracker, cfg); err != nil {
		t.Fatalf("Distribute failed: %v", err)
	}

	if maxSeenPhase1Workers.Load() > 2 {
		t.Errorf("expected Phase 1 max workers <= 2, got %d", maxSeenPhase1Workers.Load())
	}

	if maxSeenPhase2Workers.Load() > 2 {
		t.Errorf("expected Phase 2 max workers <= 2, got %d", maxSeenPhase2Workers.Load())
	}
}
