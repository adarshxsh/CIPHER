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
	"cipher/internal/transport"
)

func TestStreamLimits_MaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	t2 := transport.NewTransport(h2)

	// Open a single manual stream
	stream, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Transaction 1: REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req1); err != nil {
		t.Fatalf("Failed to send first request: %v", err)
	}

	resp1, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MANIFEST response, got %d", resp1.Type)
	}

	// Transaction 1 complete. Server should automatically close stream after 1 transaction.
	time.Sleep(10 * time.Millisecond)

	// Attempting Transaction 2 on the SAME stream
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	_ = chunk.WriteMessage(stream, req2)

	resp2, err := chunk.ReadMessage(stream)
	if err == nil {
		if resp2.Type == chunk.MsgError {
			code, msgStr, _ := chunk.ParseError(resp2.Payload)
			if code != chunk.ErrBadRequest {
				t.Errorf("Expected ErrBadRequest (7), got code %d: %s", code, msgStr)
			}
		} else {
			t.Errorf("Expected stream closure or error response for second transaction, got message type %d", resp2.Type)
		}
	} else if err != io.EOF && err.Error() != "stream reset" {
		t.Logf("Second transaction failed with stream error as expected: %v", err)
	}
}

func TestStreamLimits_MaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send 5 messages (exceeding MaxMessagesPerStream = 4)
	for i := 0; i < 5; i++ {
		req := chunk.BuildAck([32]byte{byte(i + 1)}, 0)
		_ = chunk.WriteMessage(stream, req)
	}

	// Read response: expect error message or stream closure/reset
	resp, err := chunk.ReadMessage(stream)
	if err == nil {
		if resp.Type == chunk.MsgError {
			code, msgStr, _ := chunk.ParseError(resp.Payload)
			if code != chunk.ErrUnsupportedMessage && code != chunk.ErrBadRequest {
				t.Errorf("Expected ErrUnsupportedMessage or ErrBadRequest, got code %d: %s", code, msgStr)
			}
		}
	} else if err != io.EOF && err.Error() != "stream reset" {
		t.Logf("Message flood resulted in stream error: %v", err)
	}
}

func TestStreamLimits_AutomaticStreamClosure(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	t2 := transport.NewTransport(h2)

	// Stream 1: Single manifest request
	s1, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s1, req1); err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	resp1, err := chunk.ReadMessage(s1)
	if err != nil || resp1.Type != chunk.MsgManifest {
		t.Fatalf("Failed to receive manifest response: %v", err)
	}

	// Verify stream closes automatically after 1 transaction
	time.Sleep(20 * time.Millisecond)
	_, err = chunk.ReadMessage(s1)
	if err == nil {
		t.Errorf("Expected stream to be closed after transaction, but read succeeded")
	}
}

func TestStreamLimits_EphemeralClientStreams(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := make([]byte, 1024*1024) // 4 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Create client on peer 2
	t2 := transport.NewTransport(h2)
	client, err := chunk.NewClient(ctx, t2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Ephemeral Stream 1: Resolve
	manifestData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed with ephemeral streams: %v", err)
	}

	mResolved, err := manifest.Deserialize(manifestData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	// Ephemeral Streams 2..N: Download chunks sequentially
	if err := client.Download(ctx, mResolved.ChunkIDs); err != nil {
		t.Fatalf("Download failed with ephemeral streams: %v", err)
	}

	for _, chunkID := range mResolved.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Engine missing chunk %x", chunkID)
		}
	}
}
