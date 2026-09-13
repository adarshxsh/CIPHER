package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamHandler_CopyOnWriteIsolation(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Create stream handler on h1 with 100% corruption probability
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptionProbability(1.0))
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunkID := m.ChunkIDs[0]
	storedChunkBefore, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get stored chunk before request: %v", err)
	}

	// Copy original payload to compare after request
	originalBytes := make([]byte, len(storedChunkBefore.Data))
	copy(originalBytes, storedChunkBefore.Data)

	// Request chunk from h1 with corruption enabled
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Download should fail or receive corrupted chunk on the wire
	_ = client.Download(ctx, m.ChunkIDs)

	// Verify that eng1's stored chunk was NOT modified in-place
	storedChunkAfter, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get stored chunk after request: %v", err)
	}

	if !bytes.Equal(storedChunkAfter.Data, originalBytes) {
		t.Fatalf("Engine storage was corrupted in-place! Original byte[0]=0x%x, after byte[0]=0x%x", originalBytes[0], storedChunkAfter.Data[0])
	}
}

func TestStreamHandler_ConcurrentFaultInjection(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Create stream handler on h1 with 50% corruption probability
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptionProbability(0.5))
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	const numWorkers = 8
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
			if err != nil {
				return
			}
			defer client.Close()
			_ = client.Download(ctx, m.ChunkIDs)
		}()
	}

	wg.Wait()
}
