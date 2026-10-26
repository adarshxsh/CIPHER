package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/scheduler"
	"cipher/internal/transport"
)

func createTestEngineWithDir(t testing.TB, dir string) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(dir)
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupThreeMockNodes(t testing.TB) (host.Host, host.Host, host.Host) {
	mocknet := mocknet.New()

	hClient, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hHealthy, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hCorrupt, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hClient, hHealthy, hCorrupt
}

func TestScheduler_PeerBanningAndFailover(t *testing.T) {
	hClient, hHealthy, hCorrupt := setupThreeMockNodes(t)

	dirClient := t.TempDir()
	dirHealthy := t.TempDir()
	dirCorrupt := t.TempDir()

	engClient := createTestEngineWithDir(t, dirClient)
	engHealthy := createTestEngineWithDir(t, dirHealthy)
	engCorrupt := createTestEngineWithDir(t, dirCorrupt)

	chunk.NewStreamHandler(hHealthy, engHealthy)
	chunk.NewStreamHandler(hCorrupt, engCorrupt)

	ctx := context.Background()

	// Generate test payload (~5 chunks of 32KB each = 160KB)
	payloadSize := 160 * 1024
	payload := make([]byte, payloadSize)
	rand.Read(payload)

	// Ingest payload into healthy engine
	mHealthy, err := engHealthy.Ingest(ctx, bytes.NewReader(payload), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed on healthy engine: %v", err)
	}

	// For corrupt engine: put chunks into storage, but with corrupted payload data
	for _, chunkID := range mHealthy.ChunkIDs {
		validChunk, err := engHealthy.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to get valid chunk %x: %v", chunkID, err)
		}
		badChunk := *validChunk
		badChunk.Data = append([]byte(nil), validChunk.Data...)
		if len(badChunk.Data) > 0 {
			badChunk.Data[0] ^= 0xFF // corrupt byte
		}
		if err := engCorrupt.PutChunk(ctx, &badChunk); err != nil {
			t.Fatalf("Failed to put corrupted chunk: %v", err)
		}
	}

	// Prepare ChunkTasks for scheduler
	var tasks []scheduler.ChunkTask
	for i, chunkID := range mHealthy.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  chunkID,
			Attempts: 0,
		})
	}

	// Prepare sources: corrupt peer listed first, healthy peer listed second
	sources := []scheduler.Source{
		{PeerID: hCorrupt.ID()},
		{PeerID: hHealthy.ID()},
	}

	// Custom reputation config with BanThreshold = 3, BanDuration = 5 minutes
	repCfg := scheduler.ReputationConfig{
		BanThreshold:  3.0,
		BanDuration:   5 * time.Minute,
		FaultWeight:   1.0,
		DecayHalfLife: 10 * time.Minute,
	}
	repMgr := scheduler.NewReputationManager(repCfg)

	clientTransport := transport.NewTransport(hClient)
	sched := scheduler.NewScheduler(clientTransport, engClient, 10)
	sched.Reputation = repMgr

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	close(completions)

	completedCount := 0
	for res := range completions {
		if res.Error == nil {
			completedCount++
		}
	}

	if completedCount != len(tasks) {
		t.Fatalf("Expected all %d tasks to complete, but only %d succeeded", len(tasks), completedCount)
	}

	// Verify that corrupt peer was detected and banned
	if !repMgr.IsBannedForPeer(hCorrupt.ID()) {
		t.Fatalf("Expected corrupt peer %s to be banned after invalid chunk responses", hCorrupt.ID())
	}

	// Verify that healthy peer was not banned
	if repMgr.IsBannedForPeer(hHealthy.ID()) {
		t.Fatalf("Healthy peer %s should NOT be banned", hHealthy.ID())
	}

	// Verify all chunks exist in client engine
	for _, chunkID := range mHealthy.ChunkIDs {
		has, err := engClient.HasChunk(ctx, chunkID)
		if err != nil || !has {
			t.Fatalf("Client engine missing verified chunk %x", chunkID)
		}
	}
}
