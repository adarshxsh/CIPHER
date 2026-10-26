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

func TestStreamHandler_ReadDeadline(t *testing.T) {
	origTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 50 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = origTimeout }()

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Wait longer than StreamReadTimeout without writing anything
	time.Sleep(120 * time.Millisecond)

	// Trying to read from the stream should fail because server closed it on deadline timeout
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected read to fail on timed out stream, but got nil error")
	}
}

func TestStreamHandler_MaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Open raw stream
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write transaction 1 request: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read transaction 1 response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MANIFEST, got type %d", resp1.Type)
	}

	// Wait briefly for server to terminate stream after reaching MaxTransactionsPerStream (1)
	time.Sleep(30 * time.Millisecond)

	// Transaction 2 on same stream: should fail because stream was closed by server
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	err = chunk.WriteMessage(s, req2)
	if err == nil {
		_, err = chunk.ReadMessage(s)
	}
	if err == nil {
		t.Fatalf("Expected second transaction on same stream to fail due to MaxTransactionsPerStream limit")
	}
}

func TestStreamHandler_MaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send excess messages exceeding MaxMessagesPerStream (4)
	// Build invalid message type
	dummyMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0xFF,
		Payload: []byte("test"),
	}

	var receivedError bool
	for i := 0; i < 5; i++ {
		if err := chunk.WriteMessage(s, dummyMsg); err != nil {
			break
		}
		resp, err := chunk.ReadMessage(s)
		if err != nil {
			break
		}
		if resp.Type == chunk.MsgError {
			receivedError = true
		}
	}

	if !receivedError {
		t.Fatalf("Expected to receive error message when sending invalid/excess messages")
	}

	// Further read should yield EOF or closed stream
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Errorf("Expected stream to be closed after exceeding message limits")
	} else if err != io.EOF && err.Error() != "stream reset" {
		t.Logf("Stream closed with error: %v", err)
	}
}
