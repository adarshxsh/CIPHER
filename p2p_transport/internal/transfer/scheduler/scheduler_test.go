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

func setupMockNetwork(t testing.TB, count int) ([]host.Host, func()) {
	net := mocknet.New()
	hosts := make([]host.Host, count)
	for i := 0; i < count; i++ {
		h, err := net.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := net.LinkAll(); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		for _, h := range hosts {
			_ = h.Close()
		}
	}
	return hosts, cleanup
}

func countGoroutines() int {
	return runtime.NumGoroutine()
}

func TestScheduler_SuccessfulDownload(t *testing.T) {
	hosts, cleanup := setupMockNetwork(t, 2)
	defer cleanup()

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(hosts[0], eng1)
	chunk.NewStreamHandler(hosts[1], eng2)

	ctx := context.Background()
	dataSize := 512 * 1024 // 2 chunks
	data := make([]byte, dataSize)
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
		{PeerID: hosts[0].ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(hosts[1]), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	close(completions)

	completedCount := 0
	for res := range completions {
		if res.Error != nil {
			t.Errorf("Unexpected task error: %v", res.Error)
		}
		completedCount++
	}

	if completedCount != len(tasks) {
		t.Errorf("Expected %d completed tasks, got %d", len(tasks), completedCount)
	}
}

func TestScheduler_ContextCancellation_NoGoroutineLeak(t *testing.T) {
	hosts, cleanup := setupMockNetwork(t, 3)
	defer cleanup()

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	eng3 := createTestEngine(t)

	chunk.NewStreamHandler(hosts[0], eng1)
	chunk.NewStreamHandler(hosts[1], eng2)
	chunk.NewStreamHandler(hosts[2], eng3)

	ctx := context.Background()
	dataSize := 1024 * 1024
	data := make([]byte, dataSize)
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
		{PeerID: hosts[0].ID()},
		{PeerID: hosts[1].ID()},
	}

	scheduler.TestThrottle = 50 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	cancelCtx, cancel := context.WithCancel(context.Background())
	sched := scheduler.NewScheduler(transport.NewTransport(hosts[2]), eng3, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	baselineGoroutines := countGoroutines()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(cancelCtx, tasks, sources, completions)
	}()

	// Allow workers to start active fetching
	time.Sleep(15 * time.Millisecond)

	// Cancel transfer
	cancel()

	select {
	case err := <-errCh:
		if err == nil || err != context.Canceled {
			t.Fatalf("Expected context.Canceled error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Scheduler.Run hung on context cancellation")
	}

	// Verify zero leaked goroutines (wait briefly for runtime scheduling if needed)
	deadline := time.Now().Add(500 * time.Millisecond)
	leaked := 0
	for time.Now().Before(deadline) {
		leaked = countGoroutines() - baselineGoroutines
		if leaked <= 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leaked > 0 {
		t.Errorf("Goroutine leak detected: %d additional goroutines remaining after cancellation", leaked)
	}
}

func TestScheduler_MissingChunk_ErrorTeardown(t *testing.T) {
	hosts, cleanup := setupMockNetwork(t, 2)
	defer cleanup()

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(hosts[0], eng1)
	chunk.NewStreamHandler(hosts[1], eng2)

	var fakeChunkID core.ChunkID
	copy(fakeChunkID[:], []byte("01234567890123456789012345678901"))

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: fakeChunkID},
	}

	sources := []scheduler.Source{
		{PeerID: hosts[0].ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(hosts[1]), eng2, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	baselineGoroutines := countGoroutines()

	err := sched.Run(context.Background(), tasks, sources, completions)
	if err == nil {
		t.Fatalf("Expected error for missing chunk, got nil")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	leaked := 0
	for time.Now().Before(deadline) {
		leaked = countGoroutines() - baselineGoroutines
		if leaked <= 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leaked > 0 {
		t.Errorf("Goroutine leak detected after error: %d extra goroutines running", leaked)
	}
}
