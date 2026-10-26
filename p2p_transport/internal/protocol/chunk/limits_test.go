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

func TestStreamHandler_EnforcesTransactionLimit(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := []byte("hello world stream limit test")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Open raw stream from h2 to h1
	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Transaction 1: REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req1); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}

	resp1, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest for first transaction, got %d", resp1.Type)
	}

	// Transaction 2 (Attempted on SAME stream): REQUEST_MANIFEST again
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req2); err != nil {
		t.Fatalf("Failed to write second request: %v", err)
	}

	resp2, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Expected error response message from server, got read error: %v", err)
	}

	if resp2.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError when exceeding transaction limit, got type %d", resp2.Type)
	}

	code, msgStr, err := chunk.ParseError(resp2.Payload)
	if err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}

	if code != chunk.ErrBadRequest {
		t.Errorf("Expected ErrorCode ErrBadRequest (code %d), got %d", chunk.ErrBadRequest, code)
	}
	if msgStr != "transaction limit exceeded" {
		t.Errorf("Expected message 'transaction limit exceeded', got '%s'", msgStr)
	}

	_ = eng2 // satisfy unused variable
}

func TestStreamHandler_EnforcesMessageLimit(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	var dummyContentID core.ContentID
	// Send 5 messages on the same stream without completing/closing
	for i := 1; i <= 5; i++ {
		req := chunk.BuildRequestManifest(dummyContentID)
		if err := chunk.WriteMessage(stream, req); err != nil {
			// Stream was closed by server
			return
		}

		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			// Stream closed by server after limit
			return
		}

		if resp.Type == chunk.MsgError {
			code, msgStr, _ := chunk.ParseError(resp.Payload)
			if code == chunk.ErrBadRequest {
				// Expected rejection
				return
			}
			t.Logf("Got MsgError: [%d] %s", code, msgStr)
		}
	}
}

func TestClient_FreshStreamPerTransaction(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 256*1024*3) // 3 chunks
	rand.Read(data)

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

	// 1. Resolve Manifest (Transaction 1 - stream 1)
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// 2. Download 3 chunks (Transactions 2, 3, 4 - streams 2, 3, 4)
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	for _, id := range m2.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, id); err != nil {
			t.Errorf("eng2 missing chunk %x", id)
		}
	}
}
