package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestClient_StreamResetOnHashMismatch(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Host 1 corrupts all chunk responses
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProbability(1.0))

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	chunkID := m.ChunkIDs[0]
	_, err = client.FetchChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("Expected FetchChunk to fail due to hash mismatch")
	}

	if !errors.Is(err, chunk.ErrChunkIntegrityMismatch) {
		t.Fatalf("Expected ErrChunkIntegrityMismatch, got: %v", err)
	}
}
