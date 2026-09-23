package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
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

func TestScheduler_Run_Success(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	dataSize := 256 * 1024 // 4 chunks x 64KB
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

	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	close(completions)

	completedCount := 0
	for res := range completions {
		if res.Error != nil {
			t.Errorf("Unexpected result error: %v", res.Error)
		} else {
			completedCount++
		}
	}

	if completedCount != len(tasks) {
		t.Errorf("Expected %d completed tasks, got %d", len(tasks), completedCount)
	}
}

func TestScheduler_Run_Cancellation_GoroutineLeakCheck(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx, cancel := context.WithCancel(context.Background())

	// Create 10 dummy chunk tasks that will throttle/delay
	scheduler.TestThrottle = 50 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	dataSize := 10 * 64 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := eng1.Ingest(context.Background(), bytes.NewReader(data), manifest.TypeFile)
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

	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Give workers time to start
	time.Sleep(20 * time.Millisecond)

	// Cancel transfer context
	cancel()

	select {
	case err := <-runErrCh:
		if err == nil {
			t.Fatalf("Expected cancellation error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Scheduler.Run did not exit promptly after context cancellation")
	}

	// Verify goroutines clean up
	time.Sleep(50 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()

	// Ensure no worker goroutine remains hanging
	time.Sleep(100 * time.Millisecond)
	currentGoroutines := runtime.NumGoroutine()
	if currentGoroutines > baselineGoroutines+2 {
		t.Errorf("Possible goroutine leak: baseline %d, current %d", baselineGoroutines, currentGoroutines)
	}
}

func TestScheduler_Run_MissingChunkError_Teardown(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Single missing task
	var badChunkID core.ChunkID
	badChunkID[0] = 0xFF

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: badChunkID},
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, 1)

	ctx := context.Background()
	err := sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatalf("Expected error for missing chunk, got nil")
	}
}
