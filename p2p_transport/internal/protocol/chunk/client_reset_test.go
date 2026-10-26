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

func TestClient_StreamResetOnCorruptedChunk(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Create client on Peer 2 talking to Peer 1
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Enable chunk corruption on handler responses
	chunk.TestCorruptProb = 1.0
	defer func() { chunk.TestCorruptProb = 0 }()

	// FetchChunk should detect hash mismatch, call stream.Reset(), and return ErrChunkIntegrity error
	_, fetchErr := client.FetchChunk(ctx, m.ChunkIDs[0])
	if fetchErr == nil {
		t.Fatal("Expected FetchChunk to fail on corrupted chunk")
	}

	if !errors.Is(fetchErr, chunk.ErrChunkIntegrity) {
		t.Errorf("Expected error to wrap chunk.ErrChunkIntegrity, got: %v", fetchErr)
	}
}
