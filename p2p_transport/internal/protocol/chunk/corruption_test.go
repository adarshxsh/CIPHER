package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_FaultInjectionDefensiveCopy(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Register StreamHandler on Peer 1 with 100% corruption probability option
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptionProbability(1.0))
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

	chunkID := m.ChunkIDs[0]

	// Get original chunk from eng1 before transfer
	origChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get original chunk from eng1: %v", err)
	}

	origBytes := append([]byte(nil), origChunk.Data...)

	// Peer 2 attempts to download the chunk from Peer 1
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Download should fail because Peer 1 wire handler corrupts the chunk
	err = client.Download(ctx, []core.ChunkID{chunkID})
	if err == nil {
		t.Fatalf("Expected download to fail due to chunk corruption")
	}

	// Crucial check: verify that eng1's stored chunk data in memory was NOT mutated by fault injection
	postTransferChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to retrieve chunk from eng1 post transfer: %v", err)
	}

	if !bytes.Equal(postTransferChunk.Data, origBytes) {
		t.Fatalf("Engine memory store was corrupted! Expected %v, got %v", origBytes[:10], postTransferChunk.Data[:10])
	}
}
