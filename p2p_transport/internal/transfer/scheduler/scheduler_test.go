package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
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

func setupMockNetwork(t testing.TB, nPeers int) (host.Host, []host.Host) {
	mn := mocknet.New()
	clientHost, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	peerHosts := make([]host.Host, nPeers)
	for i := 0; i < nPeers; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		peerHosts[i] = h
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return clientHost, peerHosts
}

func TestScheduler_DefaultsAndOptions(t *testing.T) {
	s := scheduler.NewScheduler(nil, nil, 3)
	if s.MaxConcurrency != scheduler.DefaultMaxConcurrency {
		t.Fatalf("expected default MaxConcurrency to be %d, got %d", scheduler.DefaultMaxConcurrency, s.MaxConcurrency)
	}

	s2 := scheduler.NewScheduler(nil, nil, 3, scheduler.WithMaxConcurrency(8))
	if s2.MaxConcurrency != 8 {
		t.Fatalf("expected MaxConcurrency to be 8, got %d", s2.MaxConcurrency)
	}
}

func TestScheduler_HighVolumePeerSetConcurrencyCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	numPeers := 25
	maxConcurrency := 4

	clientHost, providerHosts := setupMockNetwork(t, numPeers)
	clientEng := createTestEngine(t)
	providerEng := createTestEngine(t)

	// Create test data and ingest in provider engine
	data := make([]byte, 512*1024) // 8 chunks of 64KB
	rand.Read(data)

	m, err := providerEng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}

	// Register stream handlers on all provider hosts
	for _, ph := range providerHosts {
		chunk.NewStreamHandler(ph, providerEng)
	}

	clientTransport := transport.NewTransport(clientHost)

	// Build sources list with 25 candidate peers
	sources := make([]scheduler.Source, numPeers)
	for i, ph := range providerHosts {
		sources[i] = scheduler.Source{
			PeerID: ph.ID(),
		}
	}

	// Build tasks list
	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, chunkID := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		}
	}

	sched := scheduler.NewScheduler(clientTransport, clientEng, 3, scheduler.WithMaxConcurrency(maxConcurrency))
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	completedCount := 0
	for range completions {
		completedCount++
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	if completedCount != len(tasks) {
		t.Fatalf("Expected %d completed tasks, got %d", len(tasks), completedCount)
	}

	// Verify all chunks are present in client engine
	for _, chunkID := range m.ChunkIDs {
		has, err := clientEng.HasChunk(ctx, chunkID)
		if err != nil || !has {
			t.Fatalf("Client engine missing chunk %x", chunkID)
		}
	}
}

func TestScheduler_PeerFailureRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	numPeers := 10
	maxConcurrency := 2

	clientHost, providerHosts := setupMockNetwork(t, numPeers)
	clientEng := createTestEngine(t)

	// Only provider 8 and 9 have the data
	validEng := createTestEngine(t)
	emptyEng := createTestEngine(t)

	data := make([]byte, 128*1024)
	rand.Read(data)

	m, err := validEng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest: %v", err)
	}

	for i, ph := range providerHosts {
		if i >= 8 {
			chunk.NewStreamHandler(ph, validEng)
		} else {
			// First 8 peers will return ErrChunkNotFound
			chunk.NewStreamHandler(ph, emptyEng)
		}
	}

	clientTransport := transport.NewTransport(clientHost)

	sources := make([]scheduler.Source, numPeers)
	for i, ph := range providerHosts {
		sources[i] = scheduler.Source{PeerID: ph.ID()}
	}

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, chunkID := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		}
	}

	sched := scheduler.NewScheduler(clientTransport, clientEng, 3, scheduler.WithMaxConcurrency(maxConcurrency))
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	completedCount := 0
	for range completions {
		completedCount++
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Scheduler failed to recover across failing peers: %v", err)
	}

	if completedCount != len(tasks) {
		t.Fatalf("Expected %d completed tasks, got %d", len(tasks), completedCount)
	}
}

func TestScheduler_AllPeersFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	numPeers := 5
	maxConcurrency := 2

	clientHost, providerHosts := setupMockNetwork(t, numPeers)
	clientEng := createTestEngine(t)
	emptyEng := createTestEngine(t)

	for _, ph := range providerHosts {
		chunk.NewStreamHandler(ph, emptyEng)
	}

	clientTransport := transport.NewTransport(clientHost)

	sources := make([]scheduler.Source, numPeers)
	for i, ph := range providerHosts {
		sources[i] = scheduler.Source{PeerID: ph.ID()}
	}

	chunkID := core.ChunkID{0x01, 0x02}
	tasks := []scheduler.ChunkTask{{Index: 0, ChunkID: chunkID}}

	sched := scheduler.NewScheduler(clientTransport, clientEng, 3, scheduler.WithMaxConcurrency(maxConcurrency))
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err := sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatal("Expected scheduler to return error when all peers fail to provide chunk, got nil")
	}
}

type instrumentedHost struct {
	host.Host
	onStream func(s network.Stream, next network.StreamHandler)
}

func (i *instrumentedHost) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	i.Host.SetStreamHandler(pid, func(s network.Stream) {
		i.onStream(s, handler)
	})
}

func TestScheduler_StrictConcurrencyCapLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	numPeers := 30
	maxConcurrency := 3

	clientHost, providerHosts := setupMockNetwork(t, numPeers)
	clientEng := createTestEngine(t)
	providerEng := createTestEngine(t)

	data := make([]byte, 256*1024) // 4 chunks of 64KB
	rand.Read(data)

	m, err := providerEng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest: %v", err)
	}

	var activeStreams int32
	var maxActiveStreams int32

	for _, ph := range providerHosts {
		iHost := &instrumentedHost{
			Host: ph,
			onStream: func(s network.Stream, next network.StreamHandler) {
				curr := atomic.AddInt32(&activeStreams, 1)
				for {
					max := atomic.LoadInt32(&maxActiveStreams)
					if curr <= max {
						break
					}
					if atomic.CompareAndSwapInt32(&maxActiveStreams, max, curr) {
						break
					}
				}
				defer atomic.AddInt32(&activeStreams, -1)
				time.Sleep(10 * time.Millisecond) // slow down slightly to observe concurrency
				next(s)
			},
		}
		chunk.NewStreamHandler(iHost, providerEng)
	}

	clientTransport := transport.NewTransport(clientHost)

	sources := make([]scheduler.Source, numPeers)
	for i, ph := range providerHosts {
		sources[i] = scheduler.Source{PeerID: ph.ID()}
	}

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, chunkID := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{Index: i, ChunkID: chunkID}
	}

	sched := scheduler.NewScheduler(clientTransport, clientEng, 3, scheduler.WithMaxConcurrency(maxConcurrency))
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	for range completions {
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Scheduler failed: %v", err)
	}

	peak := atomic.LoadInt32(&maxActiveStreams)
	if peak > int32(maxConcurrency) {
		t.Fatalf("Strict concurrency limit violated: peak active streams was %d, expected <= %d", peak, maxConcurrency)
	}
}
