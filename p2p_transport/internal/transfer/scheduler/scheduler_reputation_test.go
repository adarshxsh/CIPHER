package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

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

func createEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupNetwork(t testing.TB) (host.Host, host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h3, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2, h3
}

func TestScheduler_3ConsecutiveChecksumFailuresBlacklistsPeer(t *testing.T) {
	hBad, _, hClient := setupNetwork(t)

	engBad := createEngine(t)
	engClient := createEngine(t)

	// hBad serves corrupted chunks (100% corruption rate)
	chunk.NewStreamHandler(hBad, engBad, chunk.WithCorruptProbability(1.0))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data := make([]byte, 1024*1024) // 4 chunks
	rand.Read(data)

	// Temporary engine to generate clean manifest & chunks
	engTmp := createEngine(t)
	m, err := engTmp.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	for _, chunkID := range m.ChunkIDs {
		chk, err := engTmp.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to get chunk from engTmp: %v", err)
		}
		if err := engBad.PutChunk(ctx, chk); err != nil {
			t.Fatalf("Failed to put chunk to engBad: %v", err)
		}
	}

	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: hBad.ID()},
	}

	repCfg := scheduler.DefaultReputationConfig()
	repCfg.BaseBackoff = 1 * time.Millisecond
	repCfg.MaxBackoff = 5 * time.Millisecond
	rep := scheduler.NewReputationManager(repCfg)

	trans := transport.NewTransport(hClient)
	sched := scheduler.NewSchedulerWithReputation(trans, engClient, 5, rep)

	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	schedErr := <-errCh
	if schedErr == nil {
		t.Fatalf("Expected scheduler to fail due to corrupted peer")
	}

	badPeerID := hBad.ID().String()
	if !rep.IsBlacklisted(badPeerID) {
		t.Fatalf("Expected bad peer %s to be blacklisted after 3 consecutive failures", badPeerID)
	}

	stats, ok := rep.GetStats(badPeerID)
	if !ok {
		t.Fatalf("Expected stats for bad peer")
	}
	if stats.ChecksumFailures < 3 {
		t.Fatalf("Expected at least 3 checksum failures for bad peer, got %d", stats.ChecksumFailures)
	}
}

func TestScheduler_MultiProviderFailoverWithCorruptedPeer(t *testing.T) {
	hGood, hBad, hClient := setupNetwork(t)

	engGood := createEngine(t)
	engBad := createEngine(t)
	engClient := createEngine(t)

	// hGood serves valid chunks
	chunk.NewStreamHandler(hGood, engGood)

	// hBad serves corrupted chunks (100% corruption rate)
	chunk.NewStreamHandler(hBad, engBad, chunk.WithCorruptProbability(1.0))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data := make([]byte, 1024*1024) // 4 chunks
	rand.Read(data)

	mGood, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest good failed: %v", err)
	}

	for _, chunkID := range mGood.ChunkIDs {
		chk, err := engGood.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to get chunk from engGood: %v", err)
		}
		if err := engBad.PutChunk(ctx, chk); err != nil {
			t.Fatalf("Failed to put chunk to engBad: %v", err)
		}
	}

	var tasks []scheduler.ChunkTask
	for i, chunkID := range mGood.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: hBad.ID()},
		{PeerID: hGood.ID()},
	}

	repCfg := scheduler.DefaultReputationConfig()
	repCfg.BaseBackoff = 1 * time.Millisecond
	repCfg.MaxBackoff = 5 * time.Millisecond
	rep := scheduler.NewReputationManager(repCfg)

	trans := transport.NewTransport(hClient)
	sched := scheduler.NewSchedulerWithReputation(trans, engClient, 5, rep)

	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	completedCount := 0
	for res := range completions {
		if res.Error == nil {
			completedCount++
		}
	}

	if schedErr := <-errCh; schedErr != nil {
		t.Fatalf("Scheduler failed unexpectedly: %v", schedErr)
	}

	if completedCount != len(mGood.ChunkIDs) {
		t.Fatalf("Expected %d completed chunks, got %d", len(mGood.ChunkIDs), completedCount)
	}

	badPeerID := hBad.ID().String()
	stats, ok := rep.GetStats(badPeerID)
	if !ok || stats.ChecksumFailures == 0 {
		t.Fatalf("Expected checksum failures recorded for bad peer %s, got %+v", badPeerID, stats)
	}
	if !rep.IsExcluded(badPeerID) {
		t.Fatalf("Expected bad peer %s to be excluded (in cooldown or blacklisted)", badPeerID)
	}

	goodPeerID := hGood.ID().String()
	if rep.IsBlacklisted(goodPeerID) {
		t.Fatalf("Expected good peer %s to remain active", goodPeerID)
	}

	for _, chunkID := range mGood.ChunkIDs {
		if _, err := engClient.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client engine missing chunk %x", chunkID)
		}
	}
}
