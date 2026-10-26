package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"runtime"
	"strings"
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

func setupMockHosts(t testing.TB, n int) []host.Host {
	mn := mocknet.New()
	hosts := make([]host.Host, n)
	for i := 0; i < n; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts
}

func TestScheduler_Success(t *testing.T) {
	hosts := setupMockHosts(t, 2)
	hClient := hosts[0]
	hPeer1 := hosts[1]

	engClient := createTestEngine(t)
	engPeer1 := createTestEngine(t)

	chunk.NewStreamHandler(hClient, engClient)
	chunk.NewStreamHandler(hPeer1, engPeer1)

	ctx := context.Background()

	// Ingest file into Peer1
	data := make([]byte, 256*1024) // 4 chunks
	rand.Read(data)

	m1, err := engPeer1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Peer1 ingest failed: %v", err)
	}

	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 3)

	var tasks []scheduler.ChunkTask
	for i, id := range m1.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  id,
			Attempts: 0,
		})
	}

	sources := []scheduler.Source{
		{PeerID: hPeer1.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	close(completions)

	count := 0
	for range completions {
		count++
	}

	if count != len(tasks) {
		t.Fatalf("Expected %d completions, got %d", len(tasks), count)
	}
}

func TestScheduler_ContextCancellation(t *testing.T) {
	hosts := setupMockHosts(t, 2)
	hClient := hosts[0]
	hPeer := hosts[1]

	engClient := createTestEngine(t)
	engPeer := createTestEngine(t)

	chunk.NewStreamHandler(hClient, engClient)
	chunk.NewStreamHandler(hPeer, engPeer)

	ctx, cancel := context.WithCancel(context.Background())

	// Create a large number of tasks
	data := make([]byte, 1024*1024) // 16 chunks
	rand.Read(data)

	m, err := engPeer.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Peer ingest failed: %v", err)
	}

	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 3)

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  id,
			Attempts: 0,
		})
	}

	sources := []scheduler.Source{
		{PeerID: hPeer.ID()},
	}

	scheduler.TestThrottle = 100 * time.Millisecond
	defer func() { scheduler.TestThrottle = 0 }()

	completions := make(chan scheduler.WorkerResult, len(tasks))

	// Cancel context after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	startGoroutines := runtime.NumGoroutine()

	err = sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatal("Expected error on canceled context, got nil")
	}

	// Give goroutines time to unwind and verify WaitGroup cleaned up all workers
	time.Sleep(200 * time.Millisecond)

	endGoroutines := runtime.NumGoroutine()
	if endGoroutines > startGoroutines+2 { // Allow small test framework variance
		t.Fatalf("Potential goroutine leak: started with %d, ended with %d", startGoroutines, endGoroutines)
	}
}

func TestScheduler_MaxAttemptsExceeded(t *testing.T) {
	hosts := setupMockHosts(t, 2)
	hClient := hosts[0]
	hPeer := hosts[1]

	engClient := createTestEngine(t)
	engPeer := createTestEngine(t)

	chunk.NewStreamHandler(hClient, engClient)
	chunk.NewStreamHandler(hPeer, engPeer)

	ctx := context.Background()

	data := []byte("test payload data for corruption test")
	m, err := engPeer.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Peer ingest failed: %v", err)
	}

	chk, err := engPeer.GetChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}
	chk.Data[0] ^= 0xFF
	if err := engPeer.PutChunk(ctx, chk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 1)

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: m.ChunkIDs[0], Attempts: 0},
	}

	sources := []scheduler.Source{
		{PeerID: hPeer.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatal("Expected error when max attempts exceeded, got nil")
	}
	expectedMsg := fmt.Sprintf("failed after %d attempts", sched.MaxAttempts)
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Fatalf("Expected error message containing %q, got %v", expectedMsg, err)
	}
}
