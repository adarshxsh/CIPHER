package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

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

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 64 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockNetwork3(t testing.TB) (host.Host, host.Host, host.Host) {
	mn := mocknet.New()

	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h3, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2, h3
}

func TestScheduler_HealthyAndFaultyPeers(t *testing.T) {
	hFaulty, hHealthy, hClient := setupMockNetwork3(t)

	engFaulty := createTestEngine(t)
	engHealthy := createTestEngine(t)
	engClient := createTestEngine(t)

	// Setup stream handlers
	handlerFaulty := chunk.NewStreamHandler(hFaulty, engFaulty)
	handlerFaulty.SetCorruptProbability(1.0) // Always corrupt chunks from faulty peer

	_ = chunk.NewStreamHandler(hHealthy, engHealthy)
	_ = chunk.NewStreamHandler(hClient, engClient)

	// Ingest test content on both providers
	ctx := context.Background()
	data := make([]byte, 256*1024) // 4 chunks of 64KB
	rand.Read(data)

	m, err := engHealthy.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Put chunks into engFaulty too so it attempts to serve them
	for _, chunkID := range m.ChunkIDs {
		c, err := engHealthy.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("GetChunk failed: %v", err)
		}
		if err := engFaulty.PutChunk(ctx, c); err != nil {
			t.Fatalf("PutChunk failed: %v", err)
		}
	}

	// Prepare tasks and sources
	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  id,
			Attempts: 0,
		})
	}

	sources := []scheduler.Source{
		{PeerID: hFaulty.ID()},
		{PeerID: hHealthy.ID()},
	}

	repCfg := scheduler.ReputationConfig{
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  1 * time.Second,
	}
	rm := scheduler.NewReputationManager(repCfg)
	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 5, rm)

	completions := make(chan scheduler.WorkerResult, len(tasks))

	runErr := sched.Run(ctx, tasks, sources, completions)
	close(completions)
	if runErr != nil {
		t.Fatalf("Scheduler.Run failed unexpectedly: %v", runErr)
	}

	completedCount := 0
	for res := range completions {
		if res.Error != nil {
			t.Errorf("Completion contained error: %v", res.Error)
		} else {
			completedCount++
		}
	}

	if completedCount != len(m.ChunkIDs) {
		t.Errorf("Expected %d completed chunks, got %d", len(m.ChunkIDs), completedCount)
	}

	// Verify Reputation Metrics
	faultyRep := rm.GetReputation(hFaulty.ID())
	if faultyRep.FailureCount == 0 {
		t.Error("Expected faulty peer to have recorded failures")
	}
	if faultyRep.ConsecutiveErrors == 0 {
		t.Error("Expected faulty peer to have consecutive errors")
	}
	if faultyRep.Score >= 1.0 {
		t.Errorf("Expected faulty peer score to drop, got %f", faultyRep.Score)
	}

	healthyRep := rm.GetReputation(hHealthy.ID())
	if healthyRep.SuccessCount == 0 {
		t.Error("Expected healthy peer to have recorded successes")
	}
	if healthyRep.ConsecutiveErrors != 0 {
		t.Errorf("Expected healthy peer consecutive errors to be 0, got %d", healthyRep.ConsecutiveErrors)
	}
}
