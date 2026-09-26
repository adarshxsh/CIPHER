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

func TestStreamHandler_DefensiveBufferCopyOnCorruption(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Register StreamHandler with 100% corruption probability on provider h1
	cfg := chunk.HandlerConfig{
		CorruptProb: 1.0,
	}
	chunk.NewStreamHandler(h1, eng1, cfg)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunkID := m.ChunkIDs[0]

	// Get original stored chunk byte slice from eng1 prior to wire transfer
	originalChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get chunk from eng1: %v", err)
	}
	originalBytes := bytes.Clone(originalChunk.Data)

	// Client fetches chunk over wire from h1
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	wireChunk, err := client.FetchChunk(ctx, chunkID)
	// Expect FetchChunk to fail checksum verification because wire bytes were corrupted
	if err == nil {
		t.Fatal("Expected FetchChunk to fail checksum verification due to fault injection, but it succeeded")
	}
	if wireChunk != nil && bytes.Equal(wireChunk.Data, originalBytes) {
		t.Fatal("Expected wire chunk data to be corrupted by fault injection, but it was identical to original")
	}

	// Verify that the chunk stored in eng1 remains UNCORRUPTED
	storedChunkAfterFetch, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to re-fetch chunk from eng1 after transfer: %v", err)
	}

	if !bytes.Equal(storedChunkAfterFetch.Data, originalBytes) {
		t.Fatalf("Memory corruption in ContentEngine! Stored chunk was mutated during fault injection transfer.\nExpected: %x\nGot: %x", originalBytes, storedChunkAfterFetch.Data)
	}
}

func TestStreamHandler_ConcurrentFaultInjectionNoDataRace(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	cfg := chunk.HandlerConfig{
		CorruptProb: 0.5,
	}
	chunk.NewStreamHandler(h1, eng1, cfg)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	chunkID := m.ChunkIDs[0]

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
			if err != nil {
				return
			}
			defer client.Close()
			_, _ = client.FetchChunk(ctx, chunkID)
		}()
	}
	wg.Wait()
}
