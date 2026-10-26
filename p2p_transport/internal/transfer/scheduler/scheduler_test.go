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
	config := core.EngineConfig{ChunkSize: 64 * 1024}
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

func TestScheduler_NormalTransfer(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

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

	trans2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(trans2, eng2, 3)

	completions := make(chan scheduler.WorkerResult, len(tasks))
	errCh := make(chan error, 1)

	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	completed := 0
	for range completions {
		completed++
		if completed == len(tasks) {
			break
		}
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	if completed != len(tasks) {
		t.Errorf("Expected %d completed chunks, got %d", len(tasks), completed)
	}
}

func TestScheduler_ContextCancellation(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx, cancel := context.WithCancel(context.Background())

	data := make([]byte, 10*1024*1024) // 10MB
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

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

	trans2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(trans2, eng2, 3)

	completions := make(chan scheduler.WorkerResult, len(tasks))
	errCh := make(chan error, 1)

	initialGoroutines := runtime.NumGoroutine()

	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Cancel context quickly
	time.Sleep(5 * time.Millisecond)
	cancel()

	err = <-errCh
	if err == nil {
		t.Fatalf("Expected error on cancelled context, got nil")
	}

	// Give worker goroutines time to finish cleanup
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines+2 {
		t.Errorf("Goroutine leak detected: before=%d, after=%d", initialGoroutines, finalGoroutines)
	}
}

func TestScheduler_UnbufferedSendWithCanceledContext(t *testing.T) {
	// Verify worker terminates promptly when ctx is canceled even if results channel is full/unbuffered and not being read.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-canceled context

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
	}
	sources := []scheduler.Source{} // No valid active workers can be started

	completions := make(chan scheduler.WorkerResult)
	sched := scheduler.NewScheduler(nil, nil, 3)

	err := sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatalf("Expected error on pre-cancelled context, got nil")
	}
}
