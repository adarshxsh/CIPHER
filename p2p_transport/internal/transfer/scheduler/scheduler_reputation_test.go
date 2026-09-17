package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

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

func createEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 64 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestScheduler_ByzantineSwarmReputation(t *testing.T) {
	mn := mocknet.New()

	hClient, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hGood, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hBad, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	engClient := createEngine(t)
	engGood := createEngine(t)
	engBad := createEngine(t)

	// Set up handlers
	chunk.NewStreamHandler(hGood, engGood)

	// Corrupt all chunks sent by hBad
	handlerBad := chunk.NewStreamHandler(hBad, engBad)
	handlerBad.CorruptProb = 1.0

	// Ingest test file into Good and Bad peers
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data := make([]byte, 1024*1024) // 16 chunks (64KB each)
	rand.Read(data)

	mGood, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data in good engine: %v", err)
	}

	// Populate engBad with the same chunks as engGood
	for _, cid := range mGood.ChunkIDs {
		cData, err := engGood.GetChunk(ctx, cid)
		if err != nil {
			t.Fatalf("Failed to get chunk %x from engGood: %v", cid, err)
		}
		if err := engBad.PutChunk(ctx, cData); err != nil {
			t.Fatalf("Failed to put chunk %x in engBad: %v", cid, err)
		}
	}

	tasks := make([]scheduler.ChunkTask, len(mGood.ChunkIDs))
	for i, cid := range mGood.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{Index: i, ChunkID: cid}
	}

	sources := []scheduler.Source{
		{PeerID: hGood.ID()},
		{PeerID: hBad.ID()},
	}

	transClient := transport.NewTransport(hClient)
	sched := scheduler.NewScheduler(transClient, engClient, 5)
	sched.Tracker.BaseBackoff = 5 * time.Millisecond // Fast backoff for testing

	completions := make(chan scheduler.WorkerResult, len(tasks)*2)
	errCh := make(chan error, 1)

	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	completedCount := 0
	for res := range completions {
		if res.Error == nil {
			completedCount++
		}
	}

	schedErr := <-errCh
	if schedErr != nil {
		t.Fatalf("Scheduler run failed: %v", schedErr)
	}

	if completedCount != len(tasks) {
		t.Fatalf("Expected %d completed chunks, got %d", len(tasks), completedCount)
	}

	// Verify reputation metrics
	badStats := sched.Tracker.GetStats(hBad.ID())
	if !badStats.IsBanned {
		t.Errorf("Expected bad peer %s to be banned, but was not", hBad.ID())
	}
	if badStats.IntegrityFailures < 2 {
		t.Errorf("Expected bad peer to have at least 2 integrity failures, got %d", badStats.IntegrityFailures)
	}
	if badStats.Score > -100 {
		t.Errorf("Expected bad peer score <= -100, got %d", badStats.Score)
	}

	goodStats := sched.Tracker.GetStats(hGood.ID())
	if goodStats.IsBanned {
		t.Errorf("Good peer should not be banned")
	}
	if goodStats.Successes < 4 {
		t.Errorf("Expected good peer to have at least 4 successes, got %d", goodStats.Successes)
	}

	// Verify client engine stored all 4 valid chunks
	for _, cid := range mGood.ChunkIDs {
		if _, err := engClient.GetChunk(ctx, cid); err != nil {
			t.Errorf("Client engine missing verified chunk %x: %v", cid, err)
		}
	}
}
