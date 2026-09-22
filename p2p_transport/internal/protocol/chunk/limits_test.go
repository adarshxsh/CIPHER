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

func TestStreamHandler_TransactionLimit(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 512)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest test data: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	t2 := transport.NewTransport(h2)
	s, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: REQUEST_MANIFEST on stream s
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write request 1: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response 1: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MANIFEST response, got type: %d", resp1.Type)
	}

	// Transaction 2: Attempt second transaction on same stream s
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		t.Fatalf("Failed to write request 2: %v", err)
	}

	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response 2: %v", err)
	}

	if resp2.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for second transaction on same stream, got type %d", resp2.Type)
	}

	code, errMsg, err := chunk.ParseError(resp2.Payload)
	if err != nil {
		t.Fatalf("Failed to parse error payload: %v", err)
	}
	if code != chunk.ErrBadRequest {
		t.Errorf("Expected ErrBadRequest code (%d), got %d", chunk.ErrBadRequest, code)
	}
	if errMsg != "transaction limit exceeded" {
		t.Errorf("Expected error message 'transaction limit exceeded', got '%s'", errMsg)
	}

	// Verify stream is closed by handler
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatal("Expected EOF after transaction limit breach error frame, but read succeeded")
	}
}

func TestClient_DedicatedStreamPerRequest(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024*1024) // 4 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// 1. Resolve Manifest (opens dedicated stream 1)
	resolvedBytes, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedBytes)
	if err != nil {
		t.Fatalf("Deserialize manifest failed: %v", err)
	}

	// 2. Download chunks (opens dedicated stream per chunk)
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}
}
