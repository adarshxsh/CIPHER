package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"

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
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockNetwork(t testing.TB, count int) []host.Host {
	net := mocknet.New()
	hosts := make([]host.Host, count)
	for i := 0; i < count; i++ {
		h, err := net.GenPeer()
		if err != nil {
			t.Fatalf("GenPeer failed: %v", err)
		}
		hosts[i] = h
	}
	if err := net.LinkAll(); err != nil {
		t.Fatalf("LinkAll failed: %v", err)
	}
	return hosts
}

func TestReputationManager_ScoringAndIsolation(t *testing.T) {
	rm := scheduler.NewReputationManager()

	peer1 := "peer1"

	// 1. Transient error (+10.0)
	rm.RecordError(peer1, errors.New("connection reset"))
	if score := rm.GetScore(peer1); score != 10.0 {
		t.Errorf("Expected score 10.0, got %.1f", score)
	}
	if rm.IsExcluded(peer1) {
		t.Errorf("Peer should not be excluded at score 10.0")
	}

	// 2. Timeout error (+25.0 -> 35.0)
	rm.RecordError(peer1, context.DeadlineExceeded)
	if score := rm.GetScore(peer1); score != 35.0 {
		t.Errorf("Expected score 35.0, got %.1f", score)
	}
	if rm.IsExcluded(peer1) {
		t.Errorf("Peer should not be excluded at score 35.0")
	}

	// 3. Success reward (-5.0 -> 30.0)
	rm.RecordSuccess(peer1)
	if score := rm.GetScore(peer1); score != 30.0 {
		t.Errorf("Expected score 30.0, got %.1f", score)
	}

	// 4. Data corruption fault (+50.0 -> 80.0 >= 50.0 IsolationThreshold)
	rm.RecordError(peer1, chunk.ErrChunkCorrupted)
	if score := rm.GetScore(peer1); score != 80.0 {
		t.Errorf("Expected score 80.0, got %.1f", score)
	}
	if !rm.IsExcluded(peer1) {
		t.Errorf("Peer should be excluded when score >= isolation threshold")
	}

	rep := rm.GetReputation(peer1)
	if rep.CorruptionCount != 1 {
		t.Errorf("Expected CorruptionCount 1, got %d", rep.CorruptionCount)
	}
	if rep.TimeoutCount != 1 {
		t.Errorf("Expected TimeoutCount 1, got %d", rep.TimeoutCount)
	}
	if rep.OtherErrorCount != 1 {
		t.Errorf("Expected OtherErrorCount 1, got %d", rep.OtherErrorCount)
	}
}

func TestScheduler_MixedQualitySwarm_IsolatesBadPeer(t *testing.T) {
	hosts := setupMockNetwork(t, 3)
	hClient := hosts[0]
	hClean := hosts[1]
	hCorrupt := hosts[2]

	engClient := createTestEngine(t)
	engClean := createTestEngine(t)
	engCorrupt := createTestEngine(t)

	chunk.NewStreamHandler(hClean, engClean)
	// Set up corrupt handler on hCorrupt only
	chunk.NewStreamHandler(hCorrupt, engCorrupt, chunk.WithCorruptProbability(1.0))

	// Ingest data into both servers
	ctx := context.Background()
	data := make([]byte, 512*1024) // 2 chunks
	rand.Read(data)

	m, err := engClean.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest clean failed: %v", err)
	}

	// Put same chunks into corrupt engine
	for _, chunkID := range m.ChunkIDs {
		c, err := engClean.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("GetChunk failed: %v", err)
		}
		if err := engCorrupt.PutChunk(ctx, c); err != nil {
			t.Fatalf("PutChunk corrupt failed: %v", err)
		}
	}

	// Build sources
	sources := []scheduler.Source{
		{PeerID: hCorrupt.ID()},
		{PeerID: hClean.ID()},
	}

	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		})
	}

	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	completed := 0
	for res := range completions {
		if res.Error == nil {
			completed++
		}
	}

	if schedErr := <-errCh; schedErr != nil {
		t.Fatalf("Scheduler.Run failed unexpectedly: %v", schedErr)
	}

	if completed != len(m.ChunkIDs) {
		t.Errorf("Expected %d completed tasks, got %d", len(m.ChunkIDs), completed)
	}

	// Verify corrupt peer was isolated
	if !sched.ReputationManager.IsExcluded(hCorrupt.ID().String()) {
		t.Errorf("Corrupt peer %s was not isolated by reputation manager", hCorrupt.ID())
	}

	// Verify clean peer remained healthy
	if sched.ReputationManager.IsExcluded(hClean.ID().String()) {
		t.Errorf("Clean peer %s was unexpectedly isolated", hClean.ID())
	}
}
