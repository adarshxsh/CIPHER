package scheduler_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
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
	mn := mocknet.New()
	hosts := make([]host.Host, count)
	for i := 0; i < count; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts
}

func TestWorker_ContextCancellation_NoGoroutineLeak(t *testing.T) {
	hosts := setupMockNetwork(t, 3)
	serverHost := hosts[0]
	clientHost := hosts[1]

	serverEngine := createTestEngine(t)
	clientEngine := createTestEngine(t)

	chunk.NewStreamHandler(serverHost, serverEngine)
	chunk.NewStreamHandler(clientHost, clientEngine)

	trans := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(trans, clientEngine, 3)

	sources := []scheduler.Source{
		{PeerID: serverHost.ID()},
	}

	var dummyChunkID core.ChunkID
	dummyChunkID[0] = 0xab

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: dummyChunkID},
		{Index: 1, ChunkID: dummyChunkID},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	// Baseline goroutine count
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatalf("Expected error due to context cancellation, got nil")
	}

	// Allow goroutines to exit completely
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines {
		t.Errorf("Goroutine leak detected: initial %d, final %d", initialGoroutines, finalGoroutines)
	}
}

func TestWorker_StreamReleaseOnCancellation(t *testing.T) {
	hosts := setupMockNetwork(t, 2)
	serverHost := hosts[0]
	clientHost := hosts[1]

	serverEngine := createTestEngine(t)
	clientEngine := createTestEngine(t)

	chunk.NewStreamHandler(serverHost, serverEngine)

	trans := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(trans, clientEngine, 3)

	sources := []scheduler.Source{
		{PeerID: serverHost.ID()},
	}

	var dummyChunkID core.ChunkID
	dummyChunkID[0] = 0xef

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: dummyChunkID},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	start := time.Now()

	go func() {
		runErrCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Trigger cancellation
	time.Sleep(10 * time.Millisecond)
	cancelStart := time.Now()
	cancel()

	err := <-runErrCh
	elapsed := time.Since(cancelStart)

	if err == nil {
		t.Errorf("Expected context cancellation error, got nil")
	}

	if elapsed > 50*time.Millisecond {
		t.Errorf("Stream/Worker release took too long: %v (expected < 50ms)", elapsed)
	}

	t.Logf("Canceled transfer released in %v (total from start: %v)", elapsed, time.Since(start))
}
