package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"runtime"
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
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupTestNodes(t *testing.T) (host.Host, host.Host, *transport.Transport, *transport.Transport, *engine.ContentEngine, *engine.ContentEngine) {
	net := mocknet.New()

	h1, err := net.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := net.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := net.LinkAll(); err != nil {
		t.Fatal(err)
	}

	t1 := transport.NewTransport(h1)
	t2 := transport.NewTransport(h2)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	return h1, h2, t1, t2, eng1, eng2
}

func TestScheduler_ContextCancellation_NoGoroutineLeak(t *testing.T) {
	_, h2, t1, _, eng1, _ := setupTestNodes(t)

	// Create a dummy chunk task
	var dummyChunkID core.ChunkID
	rand.Read(dummyChunkID[:])

	tasks := []scheduler.ChunkTask{
		{ChunkID: dummyChunkID, Index: 0, Attempts: 0},
	}

	sources := []scheduler.Source{
		{PeerID: h2.ID()},
	}

	sched := scheduler.NewScheduler(t1, eng1, 3)
	completions := make(chan scheduler.WorkerResult, 10)

	ctx, cancel := context.WithCancel(context.Background())

	// Force TestThrottle so worker sleeps inside loop, giving us time to cancel context
	scheduler.TestThrottle = 100 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	initialGoroutines := runtime.NumGoroutine()

	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Cancel context quickly
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		elapsed := time.Since(start)
		if err == nil {
			t.Fatalf("expected context error from Run, got nil")
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("Scheduler.Run took too long to abort on cancellation: %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Scheduler.Run hung after context cancellation")
	}

	// Verify goroutines clean up promptly
	time.Sleep(100 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines+2 {
		t.Fatalf("goroutine leak detected: initial %d, final %d", initialGoroutines, finalGoroutines)
	}
}

func TestScheduler_NormalOperation(t *testing.T) {
	_, h2, t1, _, eng1, eng2 := setupTestNodes(t)

	// Store data in eng2 (provider)
	ctx := context.Background()
	dataSize := 512 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, chunkID := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			ChunkID:  chunkID,
			Index:    i,
			Attempts: 0,
		}
	}

	sources := []scheduler.Source{
		{PeerID: h2.ID()},
	}

	sched := scheduler.NewScheduler(t1, eng1, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("sched.Run failed: %v", err)
	}

	close(completions)
	count := 0
	for range completions {
		count++
	}

	if count != len(tasks) {
		t.Fatalf("expected %d completions, got %d", len(tasks), count)
	}
}

func TestScheduler_RequeueAndMaxAttempts(t *testing.T) {
	_, h2, t1, _, eng1, _ := setupTestNodes(t)

	// Peer2 does NOT have the chunk data
	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	tasks := []scheduler.ChunkTask{
		{ChunkID: chunkID, Index: 0, Attempts: 0},
	}

	sources := []scheduler.Source{
		{PeerID: h2.ID()},
	}

	sched := scheduler.NewScheduler(t1, eng1, 2)
	completions := make(chan scheduler.WorkerResult, 1)

	ctx := context.Background()
	err := sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatalf("expected error due to max attempts exceeded, got nil")
	}
}
