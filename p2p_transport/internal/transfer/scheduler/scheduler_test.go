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

func TestScheduler_CancellationLeak(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Ingest 1MB file into eng1
	data := make([]byte, 1024*1024)
	rand.Read(data)
	m, err := eng1.Ingest(context.Background(), bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 3)

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		})
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	// Throttle worker to ensure tasks are in flight when cancellation occurs
	scheduler.TestThrottle = 100 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	completions := make(chan scheduler.WorkerResult)

	// Capture goroutine count before running
	initialGoroutines := runtime.NumGoroutine()

	err = sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatal("Expected scheduler to fail due to context cancellation")
	}

	// Give a short grace period for worker goroutines to shut down
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	// Goroutines should return back close to initial count (allow 1-2 tolerance for libp2p background tasks)
	if finalGoroutines-initialGoroutines > 3 {
		t.Errorf("Goroutine leak detected! Initial: %d, Final: %d", initialGoroutines, finalGoroutines)
	}
}

func TestScheduler_NormalRun(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	data := make([]byte, 512*1024) // 2 chunks
	rand.Read(data)
	m, err := eng1.Ingest(context.Background(), bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 3)

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		})
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	ctx := context.Background()
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	completedCount := 0
	for completedCount < len(tasks) {
		select {
		case res := <-completions:
			if res.Error != nil {
				t.Fatalf("Unexpected worker error: %v", res.Error)
			}
			completedCount++
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for completions")
		}
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Scheduler run failed: %v", err)
	}
}
