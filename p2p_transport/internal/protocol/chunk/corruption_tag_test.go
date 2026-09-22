//go:build corrupt_test

package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamHandler_CorruptionWithBuildTag(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Configure h1 handler with 100% corruption probability
	chunk.NewStreamHandler(h1, eng1, chunk.WithTestCorruption(1.0), chunk.WithHandlerTimeout(10*time.Second))
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 64*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// Download chunk - expected to fail due to hash mismatch from corrupted data
	err = client.Download(ctx, m.ChunkIDs)
	if err == nil {
		t.Fatal("Expected download to fail due to chunk corruption, but it succeeded")
	}
}
