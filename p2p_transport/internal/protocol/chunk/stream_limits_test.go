package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamLimits_SingleTransactionEnforced(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := []byte("hello world stream limit test")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	t2 := transport.NewTransport(h2)

	// Open raw stream manually
	s, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Transaction 1: Send REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("WriteMessage 1 failed: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("ReadMessage 1 failed: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp1.Type)
	}

	// Transaction 2 on same stream (should fail because server closed stream after 1 transaction)
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	// Write or Read should return an error as stream is closed on server side
	_ = chunk.WriteMessage(s, req2)
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected error when sending second request frame on existing stream, got nil")
	}
}

func TestStreamLimits_MaxMessagesExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	t2 := transport.NewTransport(h2)

	s, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Send 5 messages (MaxMessagesPerStream = 4)
	var badID [32]byte
	req := chunk.BuildRequestManifest(badID)

	for i := 0; i < 5; i++ {
		err := chunk.WriteMessage(s, req)
		if err != nil {
			// Stream was closed by server due to limit
			return
		}
		_, err = chunk.ReadMessage(s)
		if err != nil {
			// Server closed stream after exceeding limit
			return
		}
	}

	// If we somehow reached here without error, check if stream is closed now
	err = chunk.WriteMessage(s, req)
	if err == nil {
		_, err = chunk.ReadMessage(s)
	}
	if err == nil {
		t.Fatalf("Expected error due to exceeding MaxMessagesPerStream")
	}
}

func TestStreamLimits_ClientPerRequestStreams(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	dataSize := 1024 * 1024 // 4 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// 1. Resolve manifest (Transaction 1)
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize manifest failed: %v", err)
	}

	// 2. Fetch each chunk (Transactions 2, 3, 4, 5...)
	for _, chunkID := range m2.ChunkIDs {
		chk, err := client.FetchChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("FetchChunk failed for %x: %v", chunkID, err)
		}
		if chk == nil {
			t.Fatalf("Nil chunk returned for %x", chunkID)
		}
	}
}

func TestStreamLimits_NilTransport(t *testing.T) {
	_, err := chunk.NewClient(context.Background(), nil, "peer", nil)
	if err == nil {
		t.Fatalf("Expected error for nil transport")
	}
}

func TestStreamLimits_ClientClose(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	var dummyID [32]byte
	_, err = client.Resolve(ctx, dummyID)
	if err == nil {
		t.Fatalf("Expected error after client close")
	}
}
