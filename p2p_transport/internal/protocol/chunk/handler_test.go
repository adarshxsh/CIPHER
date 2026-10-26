package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamTransactionLimit_Enforcement(t *testing.T) {
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
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Open raw stream from h2 to h1
	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// First transaction request (Request Manifest)
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req1); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}

	resp1, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected Manifest message, got type %d", resp1.Type)
	}

	// After reaching MaxTransactionsPerStream (1), stream is closed by handler.
	// Sending a second request over the same stream must fail due to stream closure.
	req2 := chunk.BuildRequestChunk(m.ChunkIDs[0])
	_ = chunk.WriteMessage(stream, req2)

	_, err = chunk.ReadMessage(stream)
	if err == nil {
		t.Fatalf("Expected read on closed stream to fail after MaxTransactionsPerStream, but succeeded")
	}
}

func TestStreamTransactionLimit_ClientNewStreams(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes2, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes2)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Transaction 1: Resolve
	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("First transaction (Resolve) failed: %v", err)
	}

	// Transaction 2: FetchChunk (opens new stream cleanly)
	cData, err := client.FetchChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("Subsequent transaction (FetchChunk) failed: %v", err)
	}
	if len(cData.Data) == 0 {
		t.Fatalf("Fetched empty chunk data")
	}
}
