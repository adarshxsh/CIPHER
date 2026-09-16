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

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestScheduler_ContextCancellation(t *testing.T) {
	scheduler.TestThrottle = 100 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Ingest 1MB file into eng1 (4 chunks)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data := make([]byte, 1024*1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, id := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		}
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	// Get initial goroutine count
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	errCh := make(chan error, 1)

	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Allow workers to start then cancel
	time.Sleep(20 * time.Millisecond)
	cancelTime := time.Now()
	cancel()

	schedErr := <-errCh
	teardownDuration := time.Since(cancelTime)

	if schedErr != context.Canceled {
		t.Fatalf("expected context.Canceled error, got: %v", schedErr)
	}

	t.Logf("Teardown duration after cancel: %v", teardownDuration)
	if teardownDuration > 100*time.Millisecond {
		t.Errorf("Teardown duration exceeded 100ms: %v", teardownDuration)
	}

	// Verify goroutines cleaned up quickly
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	finalGoroutines := runtime.NumGoroutine()

	if finalGoroutines > initialGoroutines+2 { // allow slight buffer for system runtime routines
		t.Errorf("Leaked goroutines detected: initial %d, final %d", initialGoroutines, finalGoroutines)
	}
}

func TestScheduler_SuccessfulDownload(t *testing.T) {
	scheduler.TestThrottle = 0

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 512*1024) // 2 chunks
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, id := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		}
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("sched.Run failed: %v", err)
	}

	if len(completions) != len(tasks) {
		t.Errorf("expected %d completions, got %d", len(tasks), len(completions))
	}
}

func TestScheduler_UnbufferedCompletionsCancellation(t *testing.T) {
	scheduler.TestThrottle = 10 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx, cancel := context.WithCancel(context.Background())

	data := make([]byte, 512*1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, id := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		}
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	// Unbuffered channel
	completions := make(chan scheduler.WorkerResult)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Cancel context quickly
	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("scheduler deadlocked on canceled context with unbuffered completions channel")
	}
}
