package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestClient_ContextCancellation(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	var dummyContentID [32]byte
	_, err = client.Resolve(ctx, dummyContentID)
	if err == nil {
		t.Fatal("Expected Resolve to fail on cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got: %v", err)
	}

	var dummyChunkID [32]byte
	_, err = client.FetchChunk(ctx, dummyChunkID)
	if err == nil {
		t.Fatal("Expected FetchChunk to fail on cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got: %v", err)
	}
}

func TestClient_ContextTimeout(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Register a stalling stream handler on h1 that accepts stream but does not respond
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		time.Sleep(2 * time.Second)
		s.Close()
	})
	_ = eng1

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var dummyContentID [32]byte
	start := time.Now()
	_, err = client.Resolve(ctx, dummyContentID)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("Expected Resolve to time out")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got: %v", err)
	}
	if duration > 1*time.Second {
		t.Errorf("Resolve blocked too long: %v", duration)
	}
}

func TestClient_FetchChunkContextTimeout(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Register a stalling stream handler on h1
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		time.Sleep(2 * time.Second)
		s.Close()
	})
	_ = eng1

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var dummyChunkID [32]byte
	start := time.Now()
	_, err = client.FetchChunk(ctx, dummyChunkID)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("Expected FetchChunk to time out")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got: %v", err)
	}
	if duration > 1*time.Second {
		t.Errorf("FetchChunk blocked too long: %v", duration)
	}
}

func TestServer_AckTimeout_StalledPeer(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	// Ingest a chunk into server (eng1)
	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Host 2 manually opens stream to Host 1
	tr2 := transport.NewTransport(h2)
	s, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Host 2 requests a chunk
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to write chunk request: %v", err)
	}

	// Host 2 receives chunk response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read chunk response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got %d", resp.Type)
	}

	// Host 2 deliberately DOES NOT send ACK.
	// Host 1 should enter handleRequestChunk's ACK wait loop with AckTimeout.
	// When Host 2 closes stream (or AckTimeout expires), Host 1 finishes cleanly.
	s.Close()
}
