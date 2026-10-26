package scheduler_test

import (
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

func setupMockNetwork(t testing.TB, numPeers int) ([]host.Host, func()) {
	mn := mocknet.New()
	hosts := make([]host.Host, numPeers)
	for i := 0; i < numPeers; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		for _, h := range hosts {
			h.Close()
		}
	}
	return hosts, cleanup
}

func TestScheduler_ContextCancellation_NoGoroutineLeak(t *testing.T) {
	hosts, cleanup := setupMockNetwork(t, 4)
	defer cleanup()

	clientHost := hosts[0]
	providerHosts := hosts[1:]

	clientEng := createTestEngine(t)
	providerEngs := make([]*engine.ContentEngine, len(providerHosts))
	sources := make([]scheduler.Source, len(providerHosts))

	for i, ph := range providerHosts {
		providerEngs[i] = createTestEngine(t)
		chunk.NewStreamHandler(ph, providerEngs[i])
		chunk.NewStreamHandler(clientHost, clientEng)
		sources[i] = scheduler.Source{
			PeerID: ph.ID(),
		}
	}

	numTasks := 20
	tasks := make([]scheduler.ChunkTask, numTasks)
	for i := 0; i < numTasks; i++ {
		var chunkID core.ChunkID
		rand.Read(chunkID[:])
		tasks[i] = scheduler.ChunkTask{
			ChunkID: chunkID,
		}
	}

	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, clientEng, 3)

	ctx, cancel := context.WithCancel(context.Background())
	completions := make(chan scheduler.WorkerResult, numTasks)

	baselineGoroutines := runtime.NumGoroutine()

	errChan := make(chan error, 1)
	go func() {
		errChan <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Allow workers to start
	time.Sleep(20 * time.Millisecond)

	// Cancel transfer
	cancel()

	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("expected error due to context cancellation, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("sched.Run did not return within 1s after context cancellation")
	}

	// Give goroutines up to 100ms to exit cleanly
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baselineGoroutines+2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	currentGoroutines := runtime.NumGoroutine()
	if currentGoroutines > baselineGoroutines+5 {
		t.Fatalf("goroutine leak detected: baseline %d, current %d", baselineGoroutines, currentGoroutines)
	}
}

func TestScheduler_SuccessfulTransfer(t *testing.T) {
	hosts, cleanup := setupMockNetwork(t, 2)
	defer cleanup()

	clientHost := hosts[0]
	providerHost := hosts[1]

	clientEng := createTestEngine(t)
	providerEng := createTestEngine(t)

	chunk.NewStreamHandler(providerHost, providerEng)
	chunk.NewStreamHandler(clientHost, clientEng)

	data := []byte("hello cipher chunk data test 123")
	var rawChunkID core.ChunkID
	hash := verifier.NewSHA256Digest().Sum(data)
	copy(rawChunkID[:], hash[:])

	chunkObj := &core.Chunk{
		Header: core.ChunkHeader{ID: rawChunkID},
		Data:   data,
	}
	if err := providerEng.PutChunk(context.Background(), chunkObj); err != nil {
		t.Fatal(err)
	}

	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, clientEng, 3)

	sources := []scheduler.Source{
		{PeerID: providerHost.ID()},
	}
	tasks := []scheduler.ChunkTask{
		{ChunkID: rawChunkID},
	}

	completions := make(chan scheduler.WorkerResult, 1)
	ctx := context.Background()

	err := sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("expected successful transfer, got: %v", err)
	}

	select {
	case res := <-completions:
		if res.Error != nil {
			t.Fatalf("expected successful worker result, got error: %v", res.Error)
		}
		if res.Task.ChunkID != rawChunkID {
			t.Fatalf("unexpected chunk ID in result: %x", res.Task.ChunkID)
		}
	default:
		t.Fatal("expected completion result in channel")
	}
}
