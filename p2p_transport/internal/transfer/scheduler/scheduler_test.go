package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/transfer/scheduler"
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 64 * 1024} // 64KB chunks
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockHosts(t testing.TB, count int) []host.Host {
	mn := mocknet.New()
	hosts := make([]host.Host, count)
	for i := 0; i < count; i++ {
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

func TestScheduler_ByzantinePeerQuarantineAndReassignment(t *testing.T) {
	// Hosts: 0 = client, 1 = healthy provider, 2 = corrupt provider
	hosts := setupMockHosts(t, 3)
	clientHost := hosts[0]
	healthyHost := hosts[1]
	corruptHost := hosts[2]

	healthyEngine := createTestEngine(t)
	clientEngine := createTestEngine(t)

	// Ingest payload on healthy engine
	ctx := context.Background()
	payload := make([]byte, 256*1024) // 4 chunks
	rand.Read(payload)

	m, err := healthyEngine.Ingest(ctx, bytes.NewReader(payload), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest payload: %v", err)
	}

	// Register healthy handler
	chunk.NewStreamHandler(healthyHost, healthyEngine)

	// Register Byzantine corrupt handler on corruptHost
	corruptHost.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		for {
			reqMsg, err := chunk.ReadMessage(s)
			if err != nil {
				return
			}
			if reqMsg.Type == chunk.MsgRequestChunk {
				chunkID, _ := chunk.ParseRequestChunk(reqMsg.Payload)
				corruptData := []byte("byzantine corrupted data payload")
				respChunk := &core.Chunk{
					Header: core.ChunkHeader{ID: chunkID, PlainSize: uint32(len(corruptData)), CipherSize: uint32(len(corruptData))},
					Data:   corruptData,
				}
				msg, _ := chunk.BuildChunk(respChunk)
				if err := chunk.WriteMessage(s, msg); err != nil {
					return
				}
			}
		}
	})

	tr := reputation.NewTracker()
	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, clientEngine, 3, tr)

	var tasks []scheduler.ChunkTask
	for i, cID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: cID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: corruptHost.ID()},
		{PeerID: healthyHost.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Expected download to succeed via healthy peer reassignment, got: %v", err)
	}

	close(completions)

	completedCount := 0
	for res := range completions {
		if res.Error != nil {
			t.Fatalf("Unexpected completion error: %v", res.Error)
		}
		// Verify that all completed chunks came from healthyHost
		if res.PeerID != healthyHost.ID().String() {
			t.Errorf("Expected chunk from healthy host %s, got %s", healthyHost.ID(), res.PeerID)
		}
		completedCount++
	}

	if completedCount != len(m.ChunkIDs) {
		t.Fatalf("Expected %d completed chunks, got %d", len(m.ChunkIDs), completedCount)
	}

	// Verify corruptHost was quarantined
	if !tr.IsQuarantined(corruptHost.ID()) {
		t.Fatalf("Expected corrupt host %s to be quarantined", corruptHost.ID())
	}

	// Verify all chunks exist in client engine
	for _, cID := range m.ChunkIDs {
		has, err := clientEngine.HasChunk(ctx, cID)
		if err != nil || !has {
			t.Errorf("Client engine missing chunk %x", cID)
		}
	}
}

func TestScheduler_CandidateMissDoesNotQuarantine(t *testing.T) {
	hosts := setupMockHosts(t, 3)
	clientHost := hosts[0]
	missHost := hosts[1]
	healthyHost := hosts[2]

	healthyEngine := createTestEngine(t)
	clientEngine := createTestEngine(t)

	ctx := context.Background()
	payload := make([]byte, 128*1024) // 2 chunks
	rand.Read(payload)

	m, err := healthyEngine.Ingest(ctx, bytes.NewReader(payload), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest: %v", err)
	}

	chunk.NewStreamHandler(healthyHost, healthyEngine)

	// missHost returns ErrChunkNotFound for all requests
	missHost.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		for {
			reqMsg, err := chunk.ReadMessage(s)
			if err != nil {
				return
			}
			if reqMsg.Type == chunk.MsgRequestChunk {
				errMsg := chunk.BuildError(chunk.ErrChunkNotFound, "chunk not found")
				_ = chunk.WriteMessage(s, errMsg)
			}
		}
	})

	tr := reputation.NewTracker()
	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, clientEngine, 3, tr)

	var tasks []scheduler.ChunkTask
	for i, cID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: cID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: missHost.ID()},
		{PeerID: healthyHost.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Expected download to succeed, got: %v", err)
	}

	// Verify missHost was NOT quarantined despite candidate misses
	if tr.IsQuarantined(missHost.ID()) {
		t.Fatalf("Expected candidate miss provider %s NOT to be quarantined", missHost.ID())
	}
	if tr.GetScore(missHost.ID()) != 0 {
		t.Fatalf("Expected score 0 for candidate miss provider, got %f", tr.GetScore(missHost.ID()))
	}
}
