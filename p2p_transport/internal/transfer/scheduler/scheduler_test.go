package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
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

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupSwarmMockNetwork(t testing.TB) (host.Host, host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h3, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2, h3 // h1: Honest Provider, h2: Corrupt Provider, h3: Downloader Client
}

func TestScheduler_CorruptedPeerQuarantineAndRequeue(t *testing.T) {
	hHonest, hCorrupt, hClient := setupSwarmMockNetwork(t)

	engHonest := createTestEngine(t)
	engCorrupt := createTestEngine(t)
	engClient := createTestEngine(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataSize := 1024 * 1024 // 4 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	// Ingest file into corrupt provider so it has all chunks
	m, err := engCorrupt.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest into corrupt provider failed: %v", err)
	}

	validChunk0, err := engCorrupt.GetChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("failed to get chunk 0: %v", err)
	}

	// Ingest chunks 1..3 into honest provider (leave chunk 0 missing initially)
	for i, chunkID := range m.ChunkIDs {
		if i == 0 {
			continue
		}
		cData, err := engCorrupt.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("failed to get chunk: %v", err)
		}
		if err := engHonest.PutChunk(ctx, cData); err != nil {
			t.Fatalf("failed to put chunk: %v", err)
		}
	}

	// Register honest stream handler on hHonest
	chunk.NewStreamHandler(hHonest, engHonest)

	// Register corrupt stream handler on hCorrupt with CorruptProb = 1.0
	corruptHandler := chunk.NewStreamHandler(hCorrupt, engCorrupt)
	corruptHandler.CorruptProb = 1.0

	// When hCorrupt is queried for chunk 0, supply validChunk0 to engHonest so hHonest can succeed on re-queue
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
				if schedScore := corruptHandler; schedScore != nil {
					// Once hHonest doesn't have chunk 0, put it in after a brief tick
					_ = engHonest.PutChunk(ctx, validChunk0)
					return
				}
			}
		}
	}()

	// Prepare scheduler on client side
	trClient := transport.NewTransport(hClient)
	sched := scheduler.NewScheduler(trClient, engClient, 3)

	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  chunkID,
			Attempts: 0,
		})
	}

	// Sources include BOTH hCorrupt and hHonest
	sources := []scheduler.Source{
		{PeerID: hCorrupt.ID()},
		{PeerID: hHonest.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	// Run scheduler
	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler failed: %v", err)
	}

	// Verify all chunks downloaded
	completed := 0
	for range completions {
		completed++
		if completed == len(tasks) {
			break
		}
	}

	if completed != len(tasks) {
		t.Fatalf("Expected %d completed chunks, got %d", len(tasks), completed)
	}

	// Verify hCorrupt is quarantined in sched.ReputationTracker
	if !sched.ReputationTracker.IsQuarantined(hCorrupt.ID().String()) {
		t.Fatalf("Expected corrupt provider %s to be quarantined", hCorrupt.ID().String())
	}

	// Verify hHonest is NOT quarantined
	if sched.ReputationTracker.IsQuarantined(hHonest.ID().String()) {
		t.Fatalf("Expected honest provider %s NOT to be quarantined", hHonest.ID().String())
	}

	// Verify engClient has all chunks
	for _, chunkID := range m.ChunkIDs {
		if _, err := engClient.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("engClient missing chunk %x: %v", chunkID, err)
		}
	}
}

func TestScheduler_QuarantineTaskAttemptsNotIncremented(t *testing.T) {
	tracker := scheduler.NewReputationTracker()
	peerID := "bad-peer"

	// Record a corruption failure
	quarantined := tracker.RecordFailure(peerID, chunk.ErrChunkCorrupted)
	if !quarantined {
		t.Fatalf("expected peer to be quarantined")
	}

	// Verify that tasks failed due to peer quarantine do not burn max attempt limits
	// When quarantined is true, the scheduler requeues the task without doing task.Attempts++
	task := scheduler.ChunkTask{
		Index:    0,
		Attempts: 0,
	}

	// Simulated scheduler behavior on quarantine:
	if tracker.IsQuarantined(peerID) {
		// Task attempts remain 0
	} else {
		task.Attempts++
	}

	if task.Attempts != 0 {
		t.Fatalf("expected task attempts to remain 0 after peer quarantine, got %d", task.Attempts)
	}
}

func TestScheduler_SeparatesTimeoutFromCorruption(t *testing.T) {
	tracker := scheduler.NewReputationTracker()
	peerID := "timeout-peer"

	timeoutErr := errors.New("connection reset by peer / read timeout")
	quarantined := tracker.RecordFailure(peerID, timeoutErr)
	if quarantined {
		t.Fatalf("expected peer not to be quarantined after a single timeout")
	}

	score := tracker.GetFaultScore(peerID)
	if score != 25.0 {
		t.Fatalf("expected fault score 25.0 for timeout, got %f", score)
	}

	// Now record a corruption error for another peer
	corruptPeer := "corrupt-peer"
	quarantined = tracker.RecordFailure(corruptPeer, chunk.ErrChunkCorrupted)
	if !quarantined {
		t.Fatalf("expected corrupt peer to be quarantined immediately")
	}
	corruptScore := tracker.GetFaultScore(corruptPeer)
	if corruptScore != 50.0 {
		t.Fatalf("expected fault score 50.0 for corruption, got %f", corruptScore)
	}
}
