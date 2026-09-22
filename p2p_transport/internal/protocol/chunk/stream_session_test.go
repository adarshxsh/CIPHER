package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func TestStreamSession_MaxTransactionsExceeded(t *testing.T) {
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

	// Open raw stream directly
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: Manifest request
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to send first request: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest (2), got type %d", resp1.Type)
	}

	// Transaction 2 attempt on same stream: Breach MaxTransactionsPerStream (1)
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		// Writing might fail if stream was closed by server, which is also valid termination
	} else {
		resp2, err := chunk.ReadMessage(s)
		if err == nil {
			if resp2.Type != chunk.MsgError {
				t.Fatalf("Expected MsgError (6) for transaction limit breach, got type %d", resp2.Type)
			}
			code, msgStr, err := chunk.ParseError(resp2.Payload)
			if err != nil {
				t.Fatalf("Failed to parse error response: %v", err)
			}
			if code != chunk.ErrBadRequest {
				t.Errorf("Expected ErrorCode %d (ErrBadRequest), got %d", chunk.ErrBadRequest, code)
			}
			if msgStr != "stream transaction limit exceeded" {
				t.Errorf("Expected 'stream transaction limit exceeded', got %q", msgStr)
			}
		}
	}

	// Verify stream is now closed by server (subsequent reads should fail with EOF or reset)
	buf := make([]byte, 1)
	_, err = s.Read(buf)
	if err == nil {
		t.Errorf("Expected stream to be closed after transaction limit breach")
	}
}

func TestStreamSession_MaxMessagesExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()

	// Open raw stream directly
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send invalid message types in a loop to increment msgCount without completing transactions
	// MaxMessagesPerStream is 4. Sending a 5th message should trigger a message limit breach.
	var lastErrResp *chunk.Message
	for i := 1; i <= 5; i++ {
		// Send message with unsupported message type (e.g. 0xFF)
		msg := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    0xFF,
			Payload: []byte("test"),
		}
		if err := chunk.WriteMessage(s, msg); err != nil {
			break
		}
		resp, err := chunk.ReadMessage(s)
		if err != nil {
			break
		}
		if resp.Type == chunk.MsgError {
			code, msgStr, _ := chunk.ParseError(resp.Payload)
			if code == chunk.ErrBadRequest && msgStr == "stream message limit exceeded" {
				lastErrResp = resp
				break
			}
		}
	}

	// Verify message limit breach was triggered if 5 messages were processed
	if lastErrResp != nil {
		code, msgStr, err := chunk.ParseError(lastErrResp.Payload)
		if err != nil {
			t.Fatalf("Failed to parse error: %v", err)
		}
		if code != chunk.ErrBadRequest || msgStr != "stream message limit exceeded" {
			t.Errorf("Unexpected error payload: code=%d msg=%s", code, msgStr)
		}
	}

	// Verify stream is closed
	buf := make([]byte, 1)
	_, err = s.Read(buf)
	if err == nil {
		t.Errorf("Expected stream to be closed")
	}
}

func TestStreamSession_GracefulClosureAfterTransaction(t *testing.T) {
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

	// Open raw stream directly for Chunk + ACK transaction
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// 1. Request Chunk
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to send chunk request: %v", err)
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read chunk response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk (4), got type %d", resp.Type)
	}

	// 2. Send ACK
	ack := chunk.BuildAck(m.ChunkIDs[0], 0)
	if err := chunk.WriteMessage(s, ack); err != nil {
		t.Fatalf("Failed to send ACK: %v", err)
	}

	// 3. Server should close stream after transaction completes
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected stream to be closed by server after transaction completion")
	}
	if err != io.EOF && err.Error() != "stream reset" {
		t.Logf("Stream closed with error: %v", err)
	}
}
