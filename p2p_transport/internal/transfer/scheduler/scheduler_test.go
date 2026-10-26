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

func setupMockNetwork(t testing.TB, count int) []host.Host {
	mn := mocknet.New()
	var hosts []host.Host
	for i := 0; i < count; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, h)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts
}

func TestScheduler_CancellationNoGoroutineLeak(t *testing.T) {
	hosts := setupMockNetwork(t, 3)
	clientHost := hosts[0]
	providerHosts := hosts[1:]

	engClient := createTestEngine(t)
	engProvider1 := createTestEngine(t)
	engProvider2 := createTestEngine(t)

	chunk.NewStreamHandler(providerHosts[0], engProvider1)
	chunk.NewStreamHandler(providerHosts[1], engProvider2)
	chunk.NewStreamHandler(clientHost, engClient)

	ctx := context.Background()
	dataSize := 1024 * 1024 // 1MB = 4 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := engProvider1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	engProvider1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)
	engProvider2.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Ingest same into provider2 so both have the chunks
	m2, err := engProvider2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	_ = m2

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		})
	}

	sources := []scheduler.Source{
		{PeerID: providerHosts[0].ID()},
		{PeerID: providerHosts[1].ID()},
	}

	// Record baseline goroutine count
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	cancelCtx, cancel := context.WithCancel(context.Background())
	sched := scheduler.NewScheduler(transport.NewTransport(clientHost), engClient, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	// Introduce a small throttle so workers stay active long enough to be canceled mid-transfer
	scheduler.TestThrottle = 50 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(cancelCtx, tasks, sources, completions)
	}()

	// Cancel transfer after a tiny delay
	time.Sleep(10 * time.Millisecond)
	cancel()

	err = <-errCh
	if err == nil {
		t.Fatal("Expected error on canceled scheduler context, got nil")
	}

	// Verify goroutines settle down to initial count (or zero worker leaks)
	deadline := time.Now().Add(2 * time.Second)
	var finalGoroutines int
	for time.Now().Before(deadline) {
		runtime.GC()
		finalGoroutines = runtime.NumGoroutine()
		if finalGoroutines <= initialGoroutines {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if finalGoroutines > initialGoroutines {
		t.Errorf("Goroutine leak detected: initial %d, final %d", initialGoroutines, finalGoroutines)
	}
}

func TestScheduler_NormalExecution(t *testing.T) {
	hosts := setupMockNetwork(t, 2)
	clientHost := hosts[0]
	providerHost := hosts[1]

	engClient := createTestEngine(t)
	engProvider := createTestEngine(t)

	chunk.NewStreamHandler(providerHost, engProvider)
	chunk.NewStreamHandler(clientHost, engClient)

	ctx := context.Background()
	dataSize := 512 * 1024 // 512KB = 2 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := engProvider.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
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
		{PeerID: providerHost.ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(clientHost), engClient, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler run failed: %v", err)
	}

	if len(completions) != len(tasks) {
		t.Errorf("Expected %d completions, got %d", len(tasks), len(completions))
	}
}
