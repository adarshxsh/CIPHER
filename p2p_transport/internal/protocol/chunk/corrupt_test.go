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

func TestDefensivePayloadCloningAndCorruptOption(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Host 1 uses StreamHandler with 100% corruption probability
	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProbability(1.0))

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if len(m.ChunkIDs) == 0 {
		t.Fatalf("Expected at least 1 chunk")
	}
	targetChunkID := m.ChunkIDs[0]

	// Fetch chunk BEFORE stream handler executes to get a baseline copy of the engine's stored chunk
	originalChunk, err := eng1.GetChunk(ctx, targetChunkID)
	if err != nil {
		t.Fatalf("Failed to get original chunk: %v", err)
	}
	originalBytes := make([]byte, len(originalChunk.Data))
	copy(originalBytes, originalChunk.Data)

	// Host 2 attempts to fetch chunk from Host 1
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Fetch chunk (this will fail integrity check due to corruption)
	_, fetchErr := client.FetchChunk(ctx, targetChunkID)
	if fetchErr == nil {
		t.Fatalf("Expected FetchChunk to fail due to fault injection, but succeeded")
	}

	// Verify that the chunk stored in eng1 was NOT mutated
	storedChunk, err := eng1.GetChunk(ctx, targetChunkID)
	if err != nil {
		t.Fatalf("Failed to re-fetch stored chunk from eng1: %v", err)
	}

	if !bytes.Equal(storedChunk.Data, originalBytes) {
		t.Fatalf("Data corruption detected in engine storage! Original byte[0]=0x%x, Stored byte[0]=0x%x",
			originalBytes[0], storedChunk.Data[0])
	}
}

func TestConcurrentFaultInjection_RaceDetector(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProbability(0.5))

	ctx := context.Background()
	data := make([]byte, 512*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var wg sync.WaitGroup
	workers := 10

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
			if err != nil {
				return
			}
			defer client.Close()

			for _, cid := range m.ChunkIDs {
				_, _ = client.FetchChunk(ctx, cid)
			}
		}()
	}

	wg.Wait()
}
