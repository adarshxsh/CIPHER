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
	"cipher/internal/transport"
)

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

	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// 1st Transaction: REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req1); err != nil {
		t.Fatalf("Failed to write req1: %v", err)
	}

	resp1, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read resp1: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MANIFEST, got %d", resp1.Type)
	}

	// 2nd Transaction on SAME stream: SHOULD BE REJECTED or stream closed
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	err = chunk.WriteMessage(stream, req2)
	if err != nil {
		// Stream already closed by server after 1 transaction - OK
		return
	}

	resp2, err := chunk.ReadMessage(stream)
	if err != nil {
		// Server closed stream without reading/responding or closed after reading - OK
		if err == io.EOF || err.Error() == "stream reset" {
			return
		}
	}
	if resp2 != nil && resp2.Type == chunk.MsgError {
		// Server returned error - OK
		return
	}

	// If a response other than error was returned for 2nd transaction, fail
	if resp2 != nil && resp2.Type == chunk.MsgManifest {
		t.Fatalf("Server allowed 2nd transaction on single stream")
	}
}

func TestStreamHandler_MaxMessagesPerStream(t *testing.T) {
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

	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	req := chunk.BuildRequestManifest(m.Descriptor.ID)
	done := make(chan error, 1)

	go func() {
		for i := 0; i < 5; i++ {
			if err := chunk.WriteMessage(stream, req); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	var gotError bool
	for i := 0; i < 5; i++ {
		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			gotError = true
			break
		}
		if resp.Type == chunk.MsgError {
			gotError = true
			break
		}
	}
	<-done

	if !gotError {
		t.Fatalf("Expected stream close or error when sending multiple requests/messages on one stream")
	}
}

func TestClient_OpensNewStreamPerTransaction(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 2*256*1024) // 2 chunks
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

	// 1st Transaction: Resolve manifest
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(resolvedData) == 0 {
		t.Fatalf("Resolved empty manifest data")
	}

	// 2nd Transaction: FetchChunk #1
	chunk1, err := client.FetchChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("FetchChunk 1 failed: %v", err)
	}
	if chunk1 == nil {
		t.Fatalf("FetchChunk 1 returned nil")
	}

	// 3rd Transaction: FetchChunk #2
	chunk2, err := client.FetchChunk(ctx, m.ChunkIDs[1])
	if err != nil {
		t.Fatalf("FetchChunk 2 failed: %v", err)
	}
	if chunk2 == nil {
		t.Fatalf("FetchChunk 2 returned nil")
	}
}
