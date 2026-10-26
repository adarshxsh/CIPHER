package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"runtime"
	"testing"
	"time"

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

func TestWorker_ContextCancellation(t *testing.T) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx, cancel := context.WithCancel(context.Background())

	// Create content on eng1
	dataSize := 1024 * 1024 // 1MB
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	// Set throttle so workers take time processing chunks
	scheduler.TestThrottle = 100 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Allow worker to start and enter throttle
	time.Sleep(20 * time.Millisecond)

	// Cancel transfer context mid-execution
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Errorf("Expected error on context cancellation, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Scheduler.Run blocked on context cancellation")
	}
}

func TestWorker_GoroutineLeakOnCancel(t *testing.T) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	bgCtx := context.Background()

	// Ingest sample file
	data := make([]byte, 2*1024*1024)
	rand.Read(data)
	m, err := eng1.Ingest(bgCtx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	scheduler.TestThrottle = 200 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	// Baseline goroutine count
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(bgCtx)
	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	<-errCh

	// Give a tiny moment (< 10ms requirement) for runtime cleanup
	time.Sleep(10 * time.Millisecond)
	runtime.GC()

	currentGoroutines := runtime.NumGoroutine()
	if currentGoroutines > baselineGoroutines {
		t.Errorf("Goroutine leak detected! Baseline: %d, Current: %d", baselineGoroutines, currentGoroutines)
	}
}
