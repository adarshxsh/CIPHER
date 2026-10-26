package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
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

func setupThreePeers(t testing.TB) (host.Host, host.Host, host.Host) {
	mocknet := mocknet.New()

	hClient, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	hGood, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	hBad, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	return hClient, hGood, hBad
}

func TestScheduler_EvictsBannedWorkerAndCompletesWithHealthyWorker(t *testing.T) {
	hClient, hGood, hBad := setupThreePeers(t)

	engClient := createTestEngine(t)
	engGood := createTestEngine(t)
	engBad := createTestEngine(t)

	chunk.NewStreamHandler(hGood, engGood)
	chunk.NewStreamHandler(hBad, engBad)
	chunk.NewStreamHandler(hClient, engClient)

	ctx := context.Background()
	data := make([]byte, 512*1024) // 2 chunks
	rand.Read(data)

	// Ingest on both good and bad peers so both have the manifest/chunks
	mGood, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest good failed: %v", err)
	}
	mBad, err := engBad.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest bad failed: %v", err)
	}

	mBytesGood, _ := mGood.Serialize()
	engGood.PutManifestBytes(ctx, mGood.Descriptor.ID, mBytesGood)

	mBytesBad, _ := mBad.Serialize()
	engBad.PutManifestBytes(ctx, mBad.Descriptor.ID, mBytesBad)

	tracker := reputation.NewPeerReputationTracker()

	// Pre-ban hBad
	tracker.RecordIntegrityFault(hBad.ID())
	if !tracker.IsBanned(hBad.ID()) {
		t.Fatalf("Expected hBad to be banned")
	}

	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 3)
	sched.Tracker = tracker

	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: mGood.ChunkIDs[0], Attempts: 0},
		{Index: 1, ChunkID: mGood.ChunkIDs[1], Attempts: 0},
	}

	sources := []scheduler.Source{
		{PeerID: hBad.ID()},
		{PeerID: hGood.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Expected scheduler to complete using healthy peer, got: %v", err)
	}

	close(completions)
	completedCount := 0
	for res := range completions {
		if res.Error == nil {
			completedCount++
			if res.PeerID == hBad.ID().String() {
				t.Fatalf("Banned peer %s should not have contributed tasks", hBad.ID())
			}
		}
	}

	if completedCount != 2 {
		t.Fatalf("Expected 2 completed tasks, got %d", completedCount)
	}
}

func TestScheduler_FailsSafelyWhenAllPeersBanned(t *testing.T) {
	hClient, hGood, hBad := setupThreePeers(t)

	engClient := createTestEngine(t)
	engGood := createTestEngine(t)
	engBad := createTestEngine(t)

	chunk.NewStreamHandler(hGood, engGood)
	chunk.NewStreamHandler(hBad, engBad)

	ctx := context.Background()

	tracker := reputation.NewPeerReputationTracker()
	tracker.RecordIntegrityFault(hGood.ID())
	tracker.RecordIntegrityFault(hBad.ID())

	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 3)
	sched.Tracker = tracker

	var dummyChunkID core.ChunkID
	tasks := []scheduler.ChunkTask{
		{Index: 0, ChunkID: dummyChunkID, Attempts: 0},
	}

	sources := []scheduler.Source{
		{PeerID: hBad.ID()},
		{PeerID: hGood.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err := sched.Run(ctx, tasks, sources, completions)
	if err == nil {
		t.Fatalf("Expected scheduler to fail when all peers are banned")
	}
}
