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

func TestScheduler_ContextCancellationCleanup(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Ingest 1MB file into Peer 1 (4 chunks of 256KB)
	dataSize := 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	ctxBg := context.Background()
	m, err := eng1.Ingest(ctxBg, bytes.NewReader(data), manifest.TypeFile)
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

	// Add test throttle to ensure transfer is active when canceled
	scheduler.TestThrottle = 100 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	ctx, cancel := context.WithCancel(context.Background())

	tSched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	baselineGoroutines := runtime.NumGoroutine()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tSched.Run(ctx, tasks, sources, completions)
	}()

	// Allow worker to start fetching
	time.Sleep(20 * time.Millisecond)

	// Cancel transfer context
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("Expected context cancellation error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Scheduler.Run blocked on context cancellation")
	}

	// Verify no leaked worker goroutines
	time.Sleep(50 * time.Millisecond)
	currentGoroutines := runtime.NumGoroutine()
	if currentGoroutines > baselineGoroutines+2 { // allow small margin for runtime noise
		t.Errorf("Potential goroutine leak: baseline %d, current %d", baselineGoroutines, currentGoroutines)
	}
}

func TestScheduler_SuccessfulDownload(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	dataSize := 512 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	ctx := context.Background()
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

	tSched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = tSched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	if len(completions) != len(tasks) {
		t.Errorf("Expected %d completions, got %d", len(tasks), len(completions))
	}
}

func TestScheduler_MaxAttemptsExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	_ = createTestEngine(t)
	eng2 := createTestEngine(t)

	// Do NOT setup stream handler on h1 so requests fail

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1, 2, 3}},
	}

	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	tSched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 2)
	completions := make(chan scheduler.WorkerResult, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := tSched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatalf("Expected error when max attempts exceeded, got nil")
	}
}
