package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
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

func TestScheduler_SuccessfulTransfer(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to store content: %v", err)
	}

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: m.ChunkIDs[0]},
	}
	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 3)

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	select {
	case res := <-completions:
		if res.Error != nil {
			t.Fatalf("unexpected worker error: %v", res.Error)
		}
		if res.Task.ChunkID != m.ChunkIDs[0] {
			t.Fatalf("chunk ID mismatch: got %x, want %x", res.Task.ChunkID, m.ChunkIDs[0])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for completion result")
	}
}

func TestScheduler_Cancellation(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx, cancel := context.WithCancel(context.Background())

	// Create a non-existent task ID that will cause workers to wait or fetch
	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{0x01, 0x02, 0x03}},
	}
	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 3)

	completions := make(chan scheduler.WorkerResult, len(tasks))

	// Set throttle to simulate slow network/worker
	scheduler.TestThrottle = 100 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error on cancellation, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Scheduler.Run did not return within timeout after cancellation")
	}
}

func TestScheduler_MaxAttemptsReached(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{0xde, 0xad, 0xbe, 0xef}},
	}
	sources := []scheduler.Source{
		{PeerID: h1.ID()},
	}

	t2 := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(t2, eng2, 2)

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err := sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatal("expected error due to max attempts reached for missing chunk, got nil")
	}
}
