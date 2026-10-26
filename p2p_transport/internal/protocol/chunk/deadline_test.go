package chunk_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_ReadDeadlineTimeout(t *testing.T) {
	origReadDeadline := chunk.DefaultReadDeadline
	chunk.DefaultReadDeadline = 100 * time.Millisecond
	defer func() { chunk.DefaultReadDeadline = origReadDeadline }()

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	tp2 := transport.NewTransport(h2)

	// Open a stream to Peer 1 without sending any messages
	stream, err := tp2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Wait for handler on Peer 1 to encounter read deadline timeout and close stream
	time.Sleep(250 * time.Millisecond)

	// Attempting to read from stream should fail as remote handler closed stream after deadline expiry
	buf := make([]byte, 10)
	_, err = stream.Read(buf)
	if err == nil {
		t.Fatal("Expected read to fail on stream closed by server due to read deadline timeout")
	}
}

func TestChunkProtocol_ACKWaitDeadlineTimeout(t *testing.T) {
	origReadDeadline := chunk.DefaultReadDeadline
	chunk.DefaultReadDeadline = 100 * time.Millisecond
	defer func() { chunk.DefaultReadDeadline = origReadDeadline }()

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	ctx := context.Background()
	data := make([]byte, 1024)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunk.NewStreamHandler(h1, eng1)

	tp2 := transport.NewTransport(h2)
	stream, err := tp2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send REQUEST_CHUNK
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(stream, req); err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	// Read CHUNK response
	resp, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got %d", resp.Type)
	}

	// Do NOT send ACK! Peer 1 server should wait for ACK, hit read deadline timeout, and close stream.
	time.Sleep(250 * time.Millisecond)

	buf := make([]byte, 10)
	_, err = stream.Read(buf)
	if err == nil {
		t.Fatal("Expected read to fail after server closed stream due to ACK read deadline timeout")
	}
}
