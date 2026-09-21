package chunk_test

import (
	"context"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/transport"
)

func TestChunkProtocol_CorruptedChunkQuarantine(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng2 := createTestEngine(t)

	// Mock server stream handler on h1 that sends corrupted payload for chunk
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		reqMsg, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		if reqMsg.Type == chunk.MsgRequestChunk {
			chunkID, parseErr := chunk.ParseRequestChunk(reqMsg.Payload)
			if parseErr != nil {
				return
			}
			// Send corrupted chunk data whose SHA256 does NOT match chunkID
			corruptData := []byte("corrupted data that will fail hash verification")
			respChunk := &core.Chunk{
				Header: core.ChunkHeader{ID: chunkID, PlainSize: uint32(len(corruptData)), CipherSize: uint32(len(corruptData))},
				Data:   corruptData,
			}
			chunkMsg, _ := chunk.BuildChunk(respChunk)
			_ = chunk.WriteMessage(s, chunkMsg)
		}
	})

	ctx := context.Background()
	tr := reputation.NewTracker()

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2, chunk.WithTracker(tr))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	var testChunkID core.ChunkID
	testChunkID[0] = 0xAB

	// Attempt fetch chunk -> should fail on hash verification
	_, err = client.FetchChunk(ctx, testChunkID)
	if err == nil {
		t.Fatal("Expected error fetching corrupted chunk")
	}

	// Verify fault metric was recorded and peer h1.ID() was quarantined
	if !tr.IsQuarantined(h1.ID()) {
		t.Fatal("Expected server peer to be quarantined after sending corrupted chunk")
	}

	m := tr.GetMetrics(h1.ID())
	if m.HashFailures != 1 {
		t.Fatalf("Expected 1 hash failure metric, got %d", m.HashFailures)
	}

	// Subsequent fetch call should return ErrPeerQuarantined immediately
	_, err2 := client.FetchChunk(ctx, testChunkID)
	if err2 == nil {
		t.Fatal("Expected error on subsequent fetch from quarantined peer")
	}
	if !errors.Is(err2, chunk.ErrPeerQuarantined) {
		t.Fatalf("Expected ErrPeerQuarantined, got %v", err2)
	}
}
