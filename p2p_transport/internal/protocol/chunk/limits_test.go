package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamLimits_EnforceMaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	_ = createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	t2 := transport.NewTransport(h2)

	// Open a raw stream from h2 to h1
	s, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: REQUEST_CHUNK
	chunkID := m.ChunkIDs[0]
	req1 := chunk.BuildRequestChunk(chunkID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write request 1: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read chunk response 1: %v", err)
	}
	if resp1.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got %d", resp1.Type)
	}

	ack := chunk.BuildAck(chunkID, 0)
	if err := chunk.WriteMessage(s, ack); err != nil {
		t.Fatalf("Failed to write ACK: %v", err)
	}

	// Transaction 2 on the SAME stream: REQUEST_CHUNK
	req2 := chunk.BuildRequestChunk(chunkID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		// Stream already closed or reset
		return
	}

	resp2, err := chunk.ReadMessage(s)
	if err == nil {
		if resp2.Type == chunk.MsgError {
			code, msgStr, _ := chunk.ParseError(resp2.Payload)
			if code != chunk.ErrBadRequest {
				t.Errorf("Expected ErrBadRequest code (%d), got %d: %s", chunk.ErrBadRequest, code, msgStr)
			}
		} else {
			t.Fatalf("Expected error or stream close on 2nd transaction on same stream, got msg type %d", resp2.Type)
		}
	}
}

func TestStreamLimits_EnforceMaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	_ = createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	t2 := transport.NewTransport(h2)

	// Open raw stream
	s, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send unexpected message types to trigger limit/error
	var fakeChunkID core.ChunkID
	ackMsg := chunk.BuildAck(fakeChunkID, 0)
	if err := chunk.WriteMessage(s, ackMsg); err != nil {
		return
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		return
	}

	if resp.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError, got %d", resp.Type)
	}
	code, _, _ := chunk.ParseError(resp.Payload)
	if code != chunk.ErrUnsupportedMessage && code != chunk.ErrBadRequest {
		t.Errorf("Unexpected error code: %d", code)
	}
}

func TestStreamLimits_ClientPerTransactionStreams(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024*1024) // 4 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Client performs Resolve (Transaction 1)
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize manifest failed: %v", err)
	}

	// Client performs Download (Transactions 2, 3, 4, 5 across fresh streams)
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	for _, chunkID := range m2.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Missing chunk %x in engine 2", chunkID)
		}
	}
}
