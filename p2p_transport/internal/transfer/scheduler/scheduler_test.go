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

	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})

	return h1, h2
}

func TestScheduler_ContextCancellation(t *testing.T) {
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	h1, h2 := setupMockNetwork(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Ingest test file into eng1
	ctx := context.Background()
	data := make([]byte, 1024*1024) // 4 chunks
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

	cancelCtx, cancel := context.WithCancel(context.Background())
	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)

	// Unbuffered completions channel to simulate reader stopping / not consuming
	completions := make(chan scheduler.WorkerResult)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(cancelCtx, tasks, sources, completions)
	}()

	// Give scheduler a brief moment to start workers
	time.Sleep(10 * time.Millisecond)

	// Cancel context
	cancelStart := time.Now()
	cancel()

	select {
	case err := <-errCh:
		elapsed := time.Since(cancelStart)
		if elapsed > 100*time.Millisecond {
			t.Errorf("Scheduler.Run took %v to return after cancellation, expected <= 100ms", elapsed)
		}
		if err == nil {
			t.Errorf("Expected context error from Scheduler.Run, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Scheduler.Run did not return within timeout after context cancellation")
	}
}

func TestScheduler_UnreadCompletionsChannel_Cancellation(t *testing.T) {
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	h1, h2 := setupMockNetwork(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 512*1024) // 2 chunks
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

	cancelCtx, cancel := context.WithCancel(context.Background())
	sched := scheduler.NewScheduler(transport.NewTransport(h2), eng2, 3)

	// Unbuffered completions channel where nothing reads
	completions := make(chan scheduler.WorkerResult)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(cancelCtx, tasks, sources, completions)
	}()

	// Wait briefly then cancel
	time.Sleep(20 * time.Millisecond)
	cancelStart := time.Now()
	cancel()

	select {
	case err := <-errCh:
		elapsed := time.Since(cancelStart)
		if elapsed > 100*time.Millisecond {
			t.Errorf("Scheduler.Run took %v to unblock and return, expected <= 100ms", elapsed)
		}
		if err == nil {
			t.Errorf("Expected context error, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Scheduler.Run blocked on completions channel send despite context cancellation")
	}
}
