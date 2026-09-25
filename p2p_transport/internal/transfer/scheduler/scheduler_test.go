package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"

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

type streamCounter struct {
	network.Notifiee
	active atomic.Int64
	max    atomic.Int64
}

func (sc *streamCounter) OpenedStream(n network.Network, s network.Stream) {
	act := sc.active.Add(1)
	for {
		m := sc.max.Load()
		if act <= m {
			break
		}
		if sc.max.CompareAndSwap(m, act) {
			break
		}
	}
}

func (sc *streamCounter) ClosedStream(n network.Network, s network.Stream) {
	sc.active.Add(-1)
}

func (sc *streamCounter) Connected(network.Network, network.Conn)      {}
func (sc *streamCounter) Disconnected(network.Network, network.Conn)   {}
func (sc *streamCounter) Listen(network.Network, ma.Multiaddr)         {}
func (sc *streamCounter) ListenClose(network.Network, ma.Multiaddr)    {}

func TestScheduler_ConfigurationOptions(t *testing.T) {
	eng := createTestEngine(t)
	trans := &transport.Transport{}

	// Default MaxWorkers should be DefaultMaxWorkers (16)
	sDefault := scheduler.NewScheduler(trans, eng, 3)
	if sDefault.MaxWorkers != scheduler.DefaultMaxWorkers {
		t.Errorf("Expected default MaxWorkers=%d, got %d", scheduler.DefaultMaxWorkers, sDefault.MaxWorkers)
	}

	// WithMaxWorkers option
	sCustom := scheduler.NewScheduler(trans, eng, 3, scheduler.WithMaxWorkers(8))
	if sCustom.MaxWorkers != 8 {
		t.Errorf("Expected MaxWorkers=8, got %d", sCustom.MaxWorkers)
	}

	// Invalid value retains default
	sInvalid := scheduler.NewScheduler(trans, eng, 3, scheduler.WithMaxWorkers(-5))
	if sInvalid.MaxWorkers != scheduler.DefaultMaxWorkers {
		t.Errorf("Expected MaxWorkers=%d, got %d", scheduler.DefaultMaxWorkers, sInvalid.MaxWorkers)
	}
}

func TestScheduler_BoundedConcurrencyWithLargeSourceSet(t *testing.T) {
	mn := mocknet.New()

	clientHost, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	counter := &streamCounter{}
	clientHost.Network().Notify(counter)

	clientEngine := createTestEngine(t)
	chunk.NewStreamHandler(clientHost, clientEngine)

	// Create 100 candidate peers
	const sourceCount = 100
	var sources []scheduler.Source
	var seedEngine *engine.ContentEngine
	var seedPeerID peer.ID

	ctx := context.Background()
	dataSize := 1024 * 1024 // 1MB = 4 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	for i := 0; i < sourceCount; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		h.Network().Notify(counter)

		eng := createTestEngine(t)
		chunk.NewStreamHandler(h, eng)

		sources = append(sources, scheduler.Source{
			PeerID: h.ID(),
		})

		// Seed peer at index 50 has the data
		if i == 50 {
			seedEngine = eng
			seedPeerID = h.ID()
		}
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	m, err := seedEngine.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []scheduler.ChunkTask
	for idx, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   idx,
			ChunkID: chunkID,
		})
	}

	maxWorkers := 8
	sched := scheduler.NewScheduler(transport.NewTransport(clientHost), clientEngine, 3, scheduler.WithMaxWorkers(maxWorkers))

	completions := make(chan scheduler.WorkerResult, len(tasks))

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err = sched.Run(runCtx, tasks, sources, completions)
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

	maxObserved := counter.max.Load()
	t.Logf("Max simultaneous streams observed during transfer with %d sources: %d (MaxWorkers cap: %d)", sourceCount, maxObserved, maxWorkers)

	// Note: total streams in network (client + server) should be at most maxWorkers * 2
	if maxObserved > int64(maxWorkers*2) {
		t.Errorf("Max active libp2p streams (%d) exceeded configured MaxWorkers limit bounds (%d)", maxObserved, maxWorkers*2)
	}

	for _, chunkID := range m.ChunkIDs {
		if _, err := clientEngine.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client missing chunk %x (seed peer %s)", chunkID, seedPeerID)
		}
	}
}

func TestScheduler_WorkerSlotRecyclingOnCandidateMiss(t *testing.T) {
	mn := mocknet.New()

	clientHost, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	clientEng := createTestEngine(t)
	chunk.NewStreamHandler(clientHost, clientEng)

	// Peer A: empty engine (returns ErrRemoteChunkNotFound)
	peerAHost, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerAEng := createTestEngine(t)
	chunk.NewStreamHandler(peerAHost, peerAEng)

	// Peer B: has data
	peerBHost, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerBEng := createTestEngine(t)
	chunk.NewStreamHandler(peerBHost, peerBEng)

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	data := []byte("hello world chunk data test")
	m, err := peerBEng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	sources := []scheduler.Source{
		{PeerID: peerAHost.ID()},
		{PeerID: peerBHost.ID()},
	}

	var tasks []scheduler.ChunkTask
	for idx, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   idx,
			ChunkID: chunkID,
		})
	}

	sched := scheduler.NewScheduler(transport.NewTransport(clientHost), clientEng, 3, scheduler.WithMaxWorkers(1))
	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	close(completions)
	foundPeerB := false
	for res := range completions {
		if res.PeerID == peerBHost.ID().String() {
			foundPeerB = true
		}
	}

	if !foundPeerB {
		t.Errorf("Expected chunk to be downloaded from Peer B after Peer A candidate miss")
	}
}
