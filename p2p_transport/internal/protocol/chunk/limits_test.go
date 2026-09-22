package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamTransactionLimit_AutoCloseAndClientSeamlessReopen(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()

	// Ingest a manifest and multiple chunks on eng1
	dataSize := 1024 * 1024 // 1 MB
	data := make([]byte, dataSize)
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

	// Test Resolve Manifest (1 transaction)
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Failed to resolve manifest: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	// Download chunks - client seamlessly handles any stream limits
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	for _, chunkID := range m2.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("eng2 missing chunk %x", chunkID)
		}
	}
}

func TestStreamTransactionLimit_DirectStreamClose(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()

	// Open raw stream directly
	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	var dummyID [32]byte
	req := chunk.BuildRequestManifest(dummyID)

	// Send requests up to MaxTransactionsPerStream
	for i := 0; i < chunk.MaxTransactionsPerStream; i++ {
		if err := chunk.WriteMessage(stream, req); err != nil {
			t.Fatalf("Failed to send request %d: %v", i, err)
		}
		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			t.Fatalf("Failed to read response %d: %v", i, err)
		}
		if resp.Type != chunk.MsgError {
			t.Fatalf("Expected MsgError for unknown content, got %d", resp.Type)
		}
	}

	// The next read on the stream should either receive MsgClose or EOF because handler closed stream
	resp, err := chunk.ReadMessage(stream)
	if err == nil && resp.Type != chunk.MsgClose {
		t.Fatalf("Expected MsgClose or EOF after MaxTransactionsPerStream, got message type %d", resp.Type)
	}
}
