package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_FaultInjection_DefensiveCopy(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Server StreamHandler configured with 100% corruption probability via explicit options
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProb(1.0))
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	dataSize := 256 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Get original uncorrupted chunk from eng1 store
	origChunk, err := eng1.GetChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("Failed to get origChunk from eng1: %v", err)
	}
	origBytes := make([]byte, len(origChunk.Data))
	copy(origBytes, origChunk.Data)

	// Fetch chunk using client
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Since corruption is 100%, client hash verification should fail
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected FetchChunk to fail with hash mismatch due to corruption")
	}

	// CRITICAL: The chunk in eng1 MUST NOT be mutated!
	storeChunkAfter, err := eng1.GetChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("Failed to get storeChunkAfter: %v", err)
	}

	if !bytes.Equal(origBytes, storeChunkAfter.Data) {
		t.Fatalf("Store buffer was corrupted! Original data mutated in place!")
	}
}

func TestChunkProtocol_FaultInjection_ConcurrentRace(t *testing.T) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProb(0.5))

	ctx := context.Background()
	dataSize := 64 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	const numClients = 5
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		h2, err := mocknet.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		if err := mocknet.LinkAll(); err != nil {
			t.Fatal(err)
		}

		eng2 := createTestEngine(t)
		chunk.NewStreamHandler(h2, eng2)

		wg.Add(1)
		go func(hClient host.Host, eClient *engine.ContentEngine) {
			defer wg.Done()
			cli, err := chunk.NewClient(ctx, transport.NewTransport(hClient), h1.ID(), eClient)
			if err != nil {
				return
			}
			defer cli.Close()

			for j := 0; j < 5; j++ {
				_, _ = cli.FetchChunk(ctx, m.ChunkIDs[0])
			}
		}(h2, eng2)
	}

	wg.Wait()
}
