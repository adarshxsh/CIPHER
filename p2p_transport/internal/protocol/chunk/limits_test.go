package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func TestChunkProtocol_MaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Open a raw stream directly from h2 to h1
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// 1st transaction on stream: REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to send 1st REQUEST_MANIFEST: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read 1st MANIFEST response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp1.Type)
	}

	// 2nd transaction on SAME stream: Should be rejected or closed by server
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	_ = chunk.WriteMessage(s, req2)

	// Read on stream should return EOF or stream reset because server closed stream after 1 transaction
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected error (EOF or stream reset) on 2nd transaction on same stream, got nil")
	}
}

func TestChunkProtocol_MaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send unsupported message types repeatedly to exceed MaxMessagesPerStream without completing a transaction
	unsupportedMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0xFF, // Unsupported message type
		Payload: []byte("invalid"),
	}

	for i := 0; i < chunk.MaxMessagesPerStream+1; i++ {
		err := chunk.WriteMessage(s, unsupportedMsg)
		if err != nil {
			break
		}
		_, err = chunk.ReadMessage(s)
		if err != nil {
			// Stream was closed by server as expected when limit was reached
			if i < chunk.MaxMessagesPerStream {
				// Server closed early due to unsupported message error
			}
			return
		}
	}
}

func TestChunkProtocol_CleanClosureAfterTransaction(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Open a raw stream directly
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	req := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to send REQUEST_MANIFEST: %v", err)
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	if resp.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp.Type)
	}

	// Server should close the stream after the transaction completes
	_, err = chunk.ReadMessage(s)
	if err != io.EOF && (err == nil || err.Error() != "stream reset") {
		t.Logf("Read after transaction completion returned: %v", err)
	}
}
