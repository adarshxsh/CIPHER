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

func TestChunkProtocol_HashFailurePenalizesAndBlacklists(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Setup stream handlers
	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Peer 1 ingests data
	ctx := context.Background()
	data := make([]byte, 256*1024*4) // 4 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Create a transport with custom reputation tracker (max 3 errors)
	tracker := reputation.NewPeerTracker(reputation.Config{
		InitialScore:    100,
		PenaltyPerError: 35,
		MaxErrors:       3,
		MinScore:        0,
		MaxTrackedPeers: 100,
	})
	trans2 := transport.NewTransportWithTracker(h2, tracker)

	// Force 100% corruption on server (h1)
	chunk.TestCorruptProb = 1.0
	defer func() { chunk.TestCorruptProb = 0.0 }()

	// Client on Peer 2 fetches chunk 0
	client1, err := chunk.NewClient(ctx, trans2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client1: %v", err)
	}

	_, err = client1.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatal("Expected error fetching corrupted chunk 0")
	}
	client1.Close()

	if failures := tracker.GetHashFailures(h1.ID()); failures != 1 {
		t.Fatalf("Expected 1 hash failure, got %d", failures)
	}
	if score := tracker.GetScore(h1.ID()); score != 65 {
		t.Fatalf("Expected score 65 after 1 failure, got %d", score)
	}
	if tracker.IsBlacklisted(h1.ID()) {
		t.Fatal("Peer 1 should not be blacklisted after only 1 failure")
	}

	// Fetch chunk 1 -> 2nd failure
	client2, err := chunk.NewClient(ctx, trans2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client2: %v", err)
	}
	_, err = client2.FetchChunk(ctx, m.ChunkIDs[1])
	if err == nil {
		t.Fatal("Expected error fetching corrupted chunk 1")
	}
	client2.Close()

	if score := tracker.GetScore(h1.ID()); score != 30 {
		t.Fatalf("Expected score 30 after 2 failures, got %d", score)
	}

	// Fetch chunk 2 -> 3rd failure => Exceeds threshold!
	client3, err := chunk.NewClient(ctx, trans2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client3: %v", err)
	}
	_, err = client3.FetchChunk(ctx, m.ChunkIDs[2])
	if err == nil {
		t.Fatal("Expected error fetching corrupted chunk 2")
	}
	client3.Close()

	// Peer 1 must now be blacklisted!
	if !tracker.IsBlacklisted(h1.ID()) {
		t.Fatalf("Peer 1 should be blacklisted after 3 hash failures")
	}

	// Subsequent attempt to connect or open stream to Peer 1 MUST fail immediately
	_, err = chunk.NewClient(ctx, trans2, h1.ID(), eng2)
	if err == nil {
		t.Fatalf("Expected NewClient to fail for blacklisted peer")
	}
	if err.Error() != "peer "+h1.ID().String()+" is blacklisted" {
		t.Fatalf("Unexpected error message: %v", err)
	}
}
