package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
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

func TestScheduler_ContextCancellation(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Prepare multiple chunks on eng1
	ctx := context.Background()
	dataSize := 1024 * 1024 // 1MB -> 16 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []scheduler.ChunkTask
	for i, cid := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: cid,
		})
	}

	sources := []scheduler.Source{
		{
			PeerID: h1.ID(),
		},
	}

	// Set throttle so transfers take time
	scheduler.TestThrottle = 50 * time.Millisecond
	defer func() {
		scheduler.TestThrottle = 0
	}()

	trans := transport.NewTransport(h2)
	sched := scheduler.NewScheduler(trans, eng2, 3)

	runCtx, cancel := context.WithCancel(context.Background())
	completions := make(chan scheduler.WorkerResult) // unbuffered, no reader

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(runCtx, tasks, sources, completions)
	}()

	// Allow workers to start transfers
	time.Sleep(30 * time.Millisecond)

	baselineGoroutines := runtime.NumGoroutine()

	// Cancel the context while transfers are active
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Scheduler.Run failed to exit within timeout after context cancellation")
	}

	// Give worker goroutines time to run deferred cleanups and exit
	time.Sleep(150 * time.Millisecond)

	// Verify goroutines did not leak
	afterGoroutines := runtime.NumGoroutine()
	if afterGoroutines > baselineGoroutines+2 {
		t.Errorf("Potential goroutine leak: baseline=%d, after=%d", baselineGoroutines, afterGoroutines)
	}
}
