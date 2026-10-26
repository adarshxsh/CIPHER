package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/transport"
)

func TestChunkProtocol_CorruptedChunkResetsStreamAndPenalizesReputation(t *testing.T) {
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

	tracker := reputation.NewPeerReputationTracker()

	client, err := chunk.NewClient(
		ctx,
		transport.NewTransport(h2),
		h1.ID(),
		eng2,
		chunk.WithTracker(tracker),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Enable chunk corruption on the server
	chunk.TestCorruptProb = 1.0
	defer func() { chunk.TestCorruptProb = 0.0 }()

	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected error when fetching corrupted chunk")
	}

	// Score should be decremented by integrity penalty (-5.0)
	score := tracker.GetScore(h1.ID())
	if score >= 0.0 {
		t.Fatalf("Expected score to drop below zero, got %f", score)
	}

	if !tracker.IsBanned(h1.ID()) {
		t.Fatalf("Expected remote peer %s to be banned after corrupt chunk", h1.ID())
	}
}
