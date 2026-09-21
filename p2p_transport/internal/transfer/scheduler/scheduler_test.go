package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"runtime"
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
	config := core.EngineConfig{ChunkSize: 64 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
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
	return h1, h2
}

func TestScheduler_NormalCompletion(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 256*1024) // 4 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{Index: i, ChunkID: id})
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 3)

	completions := make(chan scheduler.WorkerResult, len(tasks))
	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Expected nil error, got %v", err)
	}

	if len(completions) != len(tasks) {
		t.Fatalf("Expected %d completion results, got %d", len(tasks), len(completions))
	}
}

func TestScheduler_ContextCancellationDuringTransfer(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Create many chunks to keep workers busy
	data := make([]byte, 512*1024)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{Index: i, ChunkID: id})
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 3)

	cancelCtx, cancel := context.WithCancel(context.Background())
	completions := make(chan scheduler.WorkerResult, len(tasks))

	// Throttle worker slightly so cancellation happens while in progress
	scheduler.SetTestThrottle(10 * time.Millisecond)
	defer func() { scheduler.SetTestThrottle(0) }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(cancelCtx, tasks, sources, completions)
	}()

	// Allow workers to start then cancel context immediately
	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Expected cancellation error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sched.Run did not return within 2s after context cancellation")
	}

	// Verify goroutine count settles down quickly
	time.Sleep(50 * time.Millisecond)
	initialRoutines := runtime.NumGoroutine()
	time.Sleep(100 * time.Millisecond)
	finalRoutines := runtime.NumGoroutine()

	if finalRoutines > initialRoutines+2 {
		t.Errorf("Potential goroutine leak: initial=%d, final=%d", initialRoutines, finalRoutines)
	}
}

func TestScheduler_UnblockedChannelSendsOnCancellation(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 128*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{Index: i, ChunkID: id})
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 3)

	// Unbuffered completions channel, nobody reading from it
	unbufferedCompletions := make(chan scheduler.WorkerResult)

	cancelCtx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(cancelCtx, tasks, sources, unbufferedCompletions)
	}()

	// Cancel context after a tiny delay
	time.Sleep(2 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Expected error on cancellation, got nil")
		}
		if err != context.Canceled {
			fmt.Printf("Scheduler returned error: %v\n", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sched.Run blocked indefinitely on channel send during context cancellation")
	}
}
