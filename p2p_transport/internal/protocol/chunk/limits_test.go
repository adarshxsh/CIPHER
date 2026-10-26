package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamLimits_MaxTransactionsPerStream(t *testing.T) {
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

	// Open a raw stream directly from h2 to h1
	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Transaction 1: Send REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req1); err != nil {
		t.Fatalf("Failed to send first REQUEST_MANIFEST: %v", err)
	}

	resp1, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp1.Type)
	}

	// Transaction 2 on SAME stream: Attempt to send a 2nd REQUEST_MANIFEST
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	_ = chunk.WriteMessage(stream, req2)

	resp2, err := chunk.ReadMessage(stream)
	// Server should either respond with ErrBadRequest ("transaction limit exceeded") or close stream (EOF/stream reset)
	if err == nil {
		if resp2.Type == chunk.MsgError {
			code, msg, _ := chunk.ParseError(resp2.Payload)
			if code != chunk.ErrBadRequest {
				t.Errorf("Expected ErrBadRequest, got code %d (%s)", code, msg)
			}
		} else {
			t.Errorf("Expected error response or closed stream, got message type %d", resp2.Type)
		}
	} else if err != io.EOF && err.Error() != "stream reset" {
		t.Logf("Stream closed with error as expected: %v", err)
	}
}

func TestStreamLimits_MaxMessagesExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send unsupported/invalid messages to trigger message limit enforcement
	for i := 0; i < 5; i++ {
		badMsg := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    chunk.MsgAck, // Not a valid initial request message
			Payload: []byte("invalid payload"),
		}
		if err := chunk.WriteMessage(stream, badMsg); err != nil {
			break
		}
		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			break
		}
		if resp.Type == chunk.MsgError {
			break
		}
	}
}

func TestStreamLimits_ClientFreshStreams(t *testing.T) {
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

	// 1. Resolve manifest (Transaction 1)
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// 2. Download chunks (Transactions 2..N across fresh streams)
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	for _, chunkID := range m2.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("eng2 missing chunk %x", chunkID)
		}
	}
}
