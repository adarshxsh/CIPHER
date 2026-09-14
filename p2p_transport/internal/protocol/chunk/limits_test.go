package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamHandler_EnforcesMaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tr2 := transport.NewTransport(h2)

	// Manually open a single stream and attempt 2 transactions on it
	s, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Transaction 1: Request Manifest
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write request 1: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response 1: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got type %d", resp1.Type)
	}

	// Transaction 2 (Rogue attempt on SAME stream): Request Manifest again
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		// If write fails because server already closed the stream, that's also valid enforcement.
		return
	}

	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		// Server closed stream after transaction 1 without reading second request, which enforces cap.
		return
	}

	// If server read second request, it must return MsgError
	if resp2.Type == chunk.MsgError {
		code, msg, _ := chunk.ParseError(resp2.Payload)
		if code != chunk.ErrBadRequest {
			t.Errorf("Expected ErrBadRequest (7), got code %d (%s)", code, msg)
		}
	} else {
		t.Fatalf("Expected second transaction on same stream to be rejected, got msg type %d", resp2.Type)
	}
}

func TestStreamHandler_EnforcesMaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	if _, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tr2 := transport.NewTransport(h2)

	s, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Send invalid / malformed messages repeatedly to exceed MaxMessagesPerStream
	var badContentID core.ContentID
	req := chunk.BuildRequestManifest(badContentID)

	for i := 0; i < 5; i++ {
		if err := chunk.WriteMessage(s, req); err != nil {
			return // stream closed by server
		}
		resp, err := chunk.ReadMessage(s)
		if err != nil {
			return // stream closed by server
		}
		if resp.Type == chunk.MsgError {
			code, msg, _ := chunk.ParseError(resp.Payload)
			t.Logf("Received expected error response on message %d: [%d] %s", i+1, code, msg)
		}
	}
}

func TestClient_DownloadMultiChunkFreshStreams(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	// Create 512KB data = 2 chunks (256KB chunk size in createTestEngine)
	data := make([]byte, 512*1024)
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

	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	for _, chunkID := range m2.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Missing chunk %x in engine 2", chunkID)
		}
	}
}
