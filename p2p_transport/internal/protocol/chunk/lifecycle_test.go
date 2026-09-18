package chunk_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_MaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	ctx := context.Background()

	// Ingest a dummy manifest in eng1
	data := []byte("test content data")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Open a raw stream from h2 to h1
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: Send REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MANIFEST response, got %d", resp1.Type)
	}

	// Transaction 2 on same stream: Send second REQUEST_MANIFEST
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	_ = chunk.WriteMessage(s, req2)

	// Since MaxTransactionsPerStream = 1, provider should have closed stream after resp1.
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected error when attempting second transaction on same stream, got none")
	}
}

func TestChunkProtocol_MaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	ctx := context.Background()

	var badID core.ContentID

	// Open a raw stream from h2 to h1
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send multiple invalid / bogus request messages to trigger message limit (>4)
	for i := 0; i < 5; i++ {
		req := chunk.BuildRequestManifest(badID)
		if err := chunk.WriteMessage(s, req); err != nil {
			// Write failed because stream was torn down by provider
			return
		}
		_, err := chunk.ReadMessage(s)
		if err != nil {
			// Stream torn down/closed as expected when message limit exceeded
			return
		}
	}

	// Attempt reading/writing one more time to confirm stream is dead
	req := chunk.BuildRequestManifest(badID)
	_ = chunk.WriteMessage(s, req)
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected stream teardown after exceeding MaxMessagesPerStream")
	}
}

func TestChunkProtocol_ClientLifecycle(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	ctx := context.Background()

	// Ingest sample data into eng1
	sampleData := []byte("sample file data for testing")
	m, err := eng1.Ingest(ctx, bytes.NewReader(sampleData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Create chunk client
	tr2 := transport.NewTransport(h2)
	client, err := chunk.NewClient(ctx, tr2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// 1. First transaction: Resolve Manifest over fresh stream
	data, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("client.Resolve failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("Resolved manifest data is empty")
	}

	// 2. Second transaction: Resolve Manifest again (should succeed over a new stream)
	data2, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("client.Resolve second call failed: %v", err)
	}
	if len(data2) == 0 {
		t.Fatalf("Resolved manifest data 2 is empty")
	}
}

func TestChunkProtocol_StalledStreamTeardown(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	ctx := context.Background()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Abruptly reset stream from client side
	_ = s.Reset()

	// Wait a moment for handler to process stream teardown
	time.Sleep(50 * time.Millisecond)
}
