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

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockHosts(t testing.TB, count int) ([]host.Host, mocknet.Mocknet) {
	mn := mocknet.New()
	hosts := make([]host.Host, count)
	for i := 0; i < count; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatalf("Failed to generate peer: %v", err)
		}
		hosts[i] = h
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatalf("Failed to link mocknet: %v", err)
	}
	return hosts, mn
}

func TestScheduler_TracksPeerFailuresAndBansByzantinePeer(t *testing.T) {
	hosts, _ := setupMockHosts(t, 3)
	clientHost := hosts[0]
	healthyHost := hosts[1]
	byzantineHost := hosts[2]

	healthyEngine := createTestEngine(t)
	byzantineEngine := createTestEngine(t)
	clientEngine := createTestEngine(t)

	// Setup normal stream handler for healthy host
	chunk.NewStreamHandler(healthyHost, healthyEngine)

	// Setup corrupt stream handler for byzantine host
	byzantineHandler := chunk.NewStreamHandler(byzantineHost, byzantineEngine)
	byzantineHandler.SetCorruptProbability(1.0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ingest 1MB content into healthy engine
	data := make([]byte, 1024*1024) // 4 chunks
	rand.Read(data)

	m, err := healthyEngine.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Copy chunks to byzantine engine so it has the same chunk IDs
	for _, chunkID := range m.ChunkIDs {
		cData, err := healthyEngine.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to get chunk %x from healthy engine: %v", chunkID, err)
		}
		if err := byzantineEngine.PutChunk(ctx, cData); err != nil {
			t.Fatalf("Failed to put chunk %x into byzantine engine: %v", chunkID, err)
		}
	}

	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, clientEngine, 3)
	sched.MaxPeerFailures = 1

	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  chunkID,
			Attempts: 0,
		})
	}

	sources := []scheduler.Source{
		{PeerID: byzantineHost.ID()},
		{PeerID: healthyHost.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	// Verify failure tracking and blacklisting
	if sched.GetPeerFailures(byzantineHost.ID()) < 1 {
		t.Errorf("Expected failure count for Byzantine peer to be >= 1, got %d", sched.GetPeerFailures(byzantineHost.ID()))
	}
	if !sched.IsBlacklisted(byzantineHost.ID()) {
		t.Errorf("Expected Byzantine peer %s to be blacklisted", byzantineHost.ID())
	}
	if sched.IsBlacklisted(healthyHost.ID()) {
		t.Errorf("Healthy peer %s should NOT be blacklisted", healthyHost.ID())
	}

	// Verify all chunks are present in client engine
	for _, chunkID := range m.ChunkIDs {
		if _, err := clientEngine.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client engine missing chunk %x", chunkID)
		}
	}
}

func TestScheduler_SwarmedDownload50PercentByzantinePeers(t *testing.T) {
	// 5 hosts: 1 client, 2 healthy peers, 2 byzantine peers
	hosts, _ := setupMockHosts(t, 5)
	clientHost := hosts[0]
	healthyHost1 := hosts[1]
	healthyHost2 := hosts[2]
	byzantineHost1 := hosts[3]
	byzantineHost2 := hosts[4]

	engClient := createTestEngine(t)
	engHealthy1 := createTestEngine(t)
	engHealthy2 := createTestEngine(t)
	engByzantine1 := createTestEngine(t)
	engByzantine2 := createTestEngine(t)

	chunk.NewStreamHandler(healthyHost1, engHealthy1)
	chunk.NewStreamHandler(healthyHost2, engHealthy2)

	byzHandler1 := chunk.NewStreamHandler(byzantineHost1, engByzantine1)
	byzHandler1.SetCorruptProbability(1.0)

	byzHandler2 := chunk.NewStreamHandler(byzantineHost2, engByzantine2)
	byzHandler2.SetCorruptProbability(1.0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ingest 2MB content (8 chunks) into healthy engines and byzantine engines
	data := make([]byte, 2*1024*1024)
	rand.Read(data)

	m, err := engHealthy1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Copy chunks to all peer engines
	for _, chunkID := range m.ChunkIDs {
		cData, err := engHealthy1.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to get chunk %x: %v", chunkID, err)
		}
		_ = engHealthy2.PutChunk(ctx, cData)
		_ = engByzantine1.PutChunk(ctx, cData)
		_ = engByzantine2.PutChunk(ctx, cData)
	}

	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, engClient, 5)
	sched.MaxPeerFailures = 1

	var tasks []scheduler.ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  chunkID,
			Attempts: 0,
		})
	}

	sources := []scheduler.Source{
		{PeerID: byzantineHost1.ID()},
		{PeerID: healthyHost1.ID()},
		{PeerID: byzantineHost2.ID()},
		{PeerID: healthyHost2.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Swarmed download with 50%% Byzantine peers failed: %v", err)
	}

	// Verify both Byzantine peers are blacklisted
	if !sched.IsBlacklisted(byzantineHost1.ID()) {
		t.Errorf("Byzantine host 1 %s should be blacklisted", byzantineHost1.ID())
	}
	if !sched.IsBlacklisted(byzantineHost2.ID()) {
		t.Errorf("Byzantine host 2 %s should be blacklisted", byzantineHost2.ID())
	}

	// Verify healthy hosts are NOT blacklisted
	if sched.IsBlacklisted(healthyHost1.ID()) {
		t.Errorf("Healthy host 1 should not be blacklisted")
	}
	if sched.IsBlacklisted(healthyHost2.ID()) {
		t.Errorf("Healthy host 2 should not be blacklisted")
	}

	// Verify client got all chunks
	for _, chunkID := range m.ChunkIDs {
		if _, err := engClient.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client missing chunk %x", chunkID)
		}
	}
}
