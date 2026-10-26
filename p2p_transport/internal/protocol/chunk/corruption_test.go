package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_EncapsulatedCorruption(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Setup Handler on Peer 1 with 100% corruption probability
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProbability(1.0))
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

	// Read original chunk data from eng1 storage before download
	originalChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get chunk from eng1: %v", err)
	}
	originalDataCopy := bytes.Clone(originalChunk.Data)

	// Peer 2 attempts to fetch chunk
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Download should fail or produce hash mismatch error due to corruption
	_, err = client.FetchChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("Expected FetchChunk to fail due to wire corruption")
	}

	// Verify that original chunk in eng1 was NOT mutated
	currentChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get chunk from eng1 after corruption test: %v", err)
	}

	if !bytes.Equal(currentChunk.Data, originalDataCopy) {
		t.Fatal("Content engine stored chunk data was mutated in-place by fault injection!")
	}
}
