package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

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
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockSwarm(t testing.TB, n int) ([]host.Host, mocknet.Mocknet) {
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
	return hosts, mn
}

func TestScheduler_BandwidthAttackMitigation(t *testing.T) {
	// 4 hosts: h0 = client, h1 = honest provider 1, h2 = honest provider 2, h3 = failing provider
	hosts, _ := setupMockSwarm(t, 4)
	clientHost := hosts[0]
	p1Host := hosts[1]
	p2Host := hosts[2]
	p3Host := hosts[3]

	clientEng := createTestEngine(t)
	p1Eng := createTestEngine(t)
	p2Eng := createTestEngine(t)
	p3Eng := createTestEngine(t) // Empty engine: missing chunks / will return errors

	// Attach chunk handlers
	chunk.NewStreamHandler(clientHost, clientEng)
	chunk.NewStreamHandler(p1Host, p1Eng)
	chunk.NewStreamHandler(p2Host, p2Eng)
	chunk.NewStreamHandler(p3Host, p3Eng)

	// Ingest 256KB payload into p1 and p2 (8 chunks of 32KB each)
	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := p1Eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Also put chunks into p2
	for _, chunkID := range m.ChunkIDs {
		cData, err := p1Eng.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to get chunk from p1: %v", err)
		}
		if err := p2Eng.PutChunk(ctx, cData); err != nil {
			t.Fatalf("Failed to put chunk to p2: %v", err)
		}
	}

	// Configure ScoreManager
	scoreCfg := reputation.DefaultConfig()
	scoreCfg.BanThreshold = 50.0
	scoreCfg.PenaltyBadRequest = 30.0
	scoreCfg.PenaltyGeneric = 30.0
	scoreMgr := reputation.NewScoreManager(scoreCfg)

	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewSchedulerWithScoreManager(clientTransport, clientEng, 3, scoreMgr)

	// Tasks
	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: chunkID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: p1Host.ID()},
		{PeerID: p2Host.ID()},
		{PeerID: p3Host.ID()}, // Malicious / failing
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Expected download to succeed via healthy peers, but got error: %v", err)
	}

	// Verify p3 is banned in ScoreManager
	if !scoreMgr.IsBanned(ctx, p3Host.ID()) {
		t.Fatalf("Expected failing peer %s to be banned in ScoreManager", p3Host.ID())
	}

	// Verify honest peers are not banned
	if scoreMgr.IsBanned(ctx, p1Host.ID()) || scoreMgr.IsBanned(ctx, p2Host.ID()) {
		t.Fatalf("Honest peers should not be banned")
	}

	// Verify all chunks received
	for _, chunkID := range m.ChunkIDs {
		has, _ := clientEng.HasChunk(ctx, chunkID)
		if !has {
			t.Fatalf("Client engine missing chunk %x", chunkID)
		}
	}
}
