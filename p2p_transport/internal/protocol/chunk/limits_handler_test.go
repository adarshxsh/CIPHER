package chunk_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestHandler_MaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	m, err := eng1.Ingest(ctx, bytes.NewReader(make([]byte, 1024)), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	t2 := transport.NewTransport(h2)
	s, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Transaction 1: Send REQUEST_MANIFEST
	req := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp.Type)
	}

	// Transaction 2 on same stream: Should fail because server closed stream after 1 transaction
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	_ = chunk.WriteMessage(s, req2)

	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected error when attempting 2nd transaction on same stream, got nil")
	}
}

func TestHandler_ValidateMessageRejection(t *testing.T) {
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

	// Send malformed REQUEST_MANIFEST (payload length 10 bytes instead of 32 bytes)
	badMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgRequestManifest,
		Payload: make([]byte, 10),
	}
	if err := chunk.WriteMessage(s, badMsg); err != nil {
		t.Fatalf("Failed to write bad message: %v", err)
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if resp.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for malformed frame, got %d", resp.Type)
	}

	code, msgStr, err := chunk.ParseError(resp.Payload)
	if err != nil {
		t.Fatalf("Failed to parse error: %v", err)
	}
	if code != chunk.ErrBadRequest {
		t.Errorf("Expected code ErrBadRequest (%d), got %d", chunk.ErrBadRequest, code)
	}
	if msgStr == "" {
		t.Errorf("Expected descriptive error message, got empty string")
	}
}

func TestClient_MultiChunkDownloadFreshStreams(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024*1024) // 4 chunks (256KB each in test engine)
	for i := range data {
		data[i] = byte(i % 256)
	}

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

	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(m2.ChunkIDs) != 4 {
		t.Fatalf("Expected 4 chunks, got %d", len(m2.ChunkIDs))
	}

	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	for _, chunkID := range m2.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("eng2 missing chunk %x", chunkID)
		}
	}
}

func TestHandler_ReadDeadlineTimeout(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	_ = serverConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	start := time.Now()
	_, err := chunk.ReadMessage(serverConn)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("Expected timeout error from ReadMessage, got nil")
	}
	if elapsed > 1*time.Second {
		t.Fatalf("ReadMessage took too long to timeout: %v", elapsed)
	}
}
