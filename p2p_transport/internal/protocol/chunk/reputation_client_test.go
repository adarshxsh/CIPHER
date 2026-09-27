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

func TestChunkClient_CorruptedChunk_ResetsStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	chunk.TestCorruptProb = 1.0
	defer func() { chunk.TestCorruptProb = 0.0 }()

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected error when fetching corrupted chunk")
	}

	if !errors.Is(err, chunk.ErrChunkCorrupted) {
		t.Errorf("Expected ErrChunkCorrupted, got: %v", err)
	}

	// Subsequent fetch on same client/stream should fail immediately due to transport stream reset
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected error on subsequent fetch after transport stream reset")
	}
}
