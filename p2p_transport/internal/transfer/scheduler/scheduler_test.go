package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/manager"
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

func TestScheduler_MaxWorkersDefaultAndConfig(t *testing.T) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	tr := transport.NewTransport(h1)
	eng := createTestEngine(t)

	// Default MaxWorkers should be 16
	sDefault := scheduler.NewScheduler(tr, eng, 3)
	if sDefault.MaxWorkers != 16 {
		t.Errorf("Expected default MaxWorkers to be 16, got %d", sDefault.MaxWorkers)
	}

	// Custom MaxWorkers
	sCustom := scheduler.NewScheduler(tr, eng, 3, scheduler.WithMaxWorkers(8))
	if sCustom.MaxWorkers != 8 {
		t.Errorf("Expected custom MaxWorkers to be 8, got %d", sCustom.MaxWorkers)
	}

	// TransferManager option configuration
	sm, err := manager.NewFileSessionManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tm := manager.NewTransferManager(sm, eng, tr, manager.WithMaxWorkers(12))
	if tm.MaxWorkers != 12 {
		t.Errorf("Expected TransferManager MaxWorkers to be 12, got %d", tm.MaxWorkers)
	}
}

func TestChunkQueue_PopForPeerAndNotifications(t *testing.T) {
	var chunkID1, chunkID2 core.ChunkID
	chunkID1[0] = 1
	chunkID2[0] = 2

	task1 := scheduler.ChunkTask{Index: 0, ChunkID: chunkID1}
	task2 := scheduler.ChunkTask{Index: 1, ChunkID: chunkID2, MissedPeers: map[string]bool{"PeerB": true}}

	queue := scheduler.NewChunkQueue([]scheduler.ChunkTask{task1, task2})

	// Peer A hasn't missed any task, PopForPeer should return task1
	t1, found, hasTasks := queue.PopForPeer("PeerA")
	if !found || !hasTasks || t1.Index != 0 {
		t.Fatalf("Expected task1 for PeerA, got found=%v, hasTasks=%v, index=%d", found, hasTasks, t1.Index)
	}

	// Queue now has task2 (missed by PeerB)
	_, foundB, hasTasksB := queue.PopForPeer("PeerB")
	if foundB {
		t.Errorf("PeerB should not pop task2 since it's in MissedPeers")
	}
	if !hasTasksB {
		t.Errorf("hasTasks should be true because task2 is still in queue")
	}

	// Peer C (not in MissedPeers) pops for peer
	t2, foundC, hasTasksC := queue.PopForPeer("PeerC")
	if !foundC || !hasTasksC || t2.Index != 1 {
		t.Errorf("Expected task2 for PeerC, got found=%v, hasTasks=%v, index=%d", foundC, hasTasksC, t2.Index)
	}
}

func TestScheduler_CleanShutdownUnderLargeSourceList(t *testing.T) {
	mocknet := mocknet.New()
	clientHost, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	peerEng := createTestEngine(t)
	var sources []scheduler.Source
	for i := 0; i < 50; i++ {
		peerHost, err := mocknet.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		chunk.NewStreamHandler(peerHost, peerEng)
		sources = append(sources, scheduler.Source{PeerID: peerHost.ID()})
	}
	_ = mocknet.LinkAll()

	tr := transport.NewTransport(clientHost)
	eng := createTestEngine(t)
	sched := scheduler.NewScheduler(tr, eng, 3, scheduler.WithMaxWorkers(8))

	var chunkID core.ChunkID
	chunkID[0] = 99
	tasks := []scheduler.ChunkTask{{Index: 0, ChunkID: chunkID}}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	completions := make(chan scheduler.WorkerResult, 10)
	err = sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Errorf("Expected context error due to cancellation/timeout, got nil")
	}
}

func TestScheduler_Integration100Peers(t *testing.T) {
	mocknet := mocknet.New()
	clientHost, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	clientEng := createTestEngine(t)
	tr := transport.NewTransport(clientHost)
	chunk.NewStreamHandler(clientHost, clientEng)

	// Create 100 provider peer sources
	numProviders := 100
	var sources []scheduler.Source

	// Host 0 will store the actual chunk
	providerEngines := make([]*engine.ContentEngine, numProviders)

	data := []byte("hello world 100 peer swarm test content")
	dig := verifier.NewSHA256Digest()
	chunkID := core.ChunkID(dig.Sum(data))
	testChunk := &core.Chunk{
		Header: core.ChunkHeader{
			ID:         chunkID,
			CipherSize: uint32(len(data)),
			PlainSize:  uint32(len(data)),
		},
		Data: data,
	}

	ctx := context.Background()

	for i := 0; i < numProviders; i++ {
		pHost, err := mocknet.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		pEng := createTestEngine(t)
		chunk.NewStreamHandler(pHost, pEng)
		providerEngines[i] = pEng
		sources = append(sources, scheduler.Source{PeerID: pHost.ID()})
	}

	// Put chunk in the last provider (provider 99)
	if err := providerEngines[99].PutChunk(ctx, testChunk); err != nil {
		t.Fatalf("Failed to put chunk in provider: %v", err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	sched := scheduler.NewScheduler(tr, clientEng, 3, scheduler.WithMaxWorkers(16))
	tasks := []scheduler.ChunkTask{{Index: 0, ChunkID: chunkID}}

	completions := make(chan scheduler.WorkerResult, len(tasks))
	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed with 100 peer sources: %v", err)
	}

	res := <-completions
	if res.Task.ChunkID != chunkID {
		t.Errorf("Expected chunkID %x, got %x", chunkID, res.Task.ChunkID)
	}

	// Verify client engine has the chunk
	has, err := clientEng.HasChunk(ctx, chunkID)
	if err != nil || !has {
		t.Errorf("Expected client engine to have downloaded chunk, has=%v, err=%v", has, err)
	}
}
