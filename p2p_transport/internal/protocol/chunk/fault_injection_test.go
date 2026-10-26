package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_FaultInjectionDefensiveCopy(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Create handler with 1.0 corruption probability on h1
	handler := chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProb(1.0))
	chunk.NewStreamHandler(h2, eng2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunkID := m.ChunkIDs[0]
	origChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get orig chunk: %v", err)
	}

	// Copy original data bytes to compare later
	origBytes := make([]byte, len(origChunk.Data))
	copy(origBytes, origChunk.Data)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Fetch chunk - should receive corrupt data
	fetchedChunk, err := client.FetchChunk(ctx, chunkID)
	if err == nil {
		// If verification failed inside FetchChunk or if bytes returned, let's check
		if bytes.Equal(fetchedChunk.Data, origBytes) {
			t.Fatalf("Expected fetched chunk to be corrupted, but matched original")
		}
	}

	// Crucial check: Host 1's engine chunk buffer MUST NOT be corrupted!
	afterChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to re-fetch chunk from eng1: %v", err)
	}

	if !bytes.Equal(afterChunk.Data, origBytes) {
		t.Fatalf("Store memory was mutated by fault injection! Expected %v, got %v", origBytes[:10], afterChunk.Data[:10])
	}

	// Test dynamic setter
	handler.SetCorruptProb(0.0)
	if handler.CorruptProb() != 0.0 {
		t.Fatalf("Expected CorruptProb 0.0, got %f", handler.CorruptProb())
	}
}

func TestChunkProtocol_ThreadSafeOptions(t *testing.T) {
	h1, _ := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProb(0.1))

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(val float64) {
			defer wg.Done()
			handler.SetCorruptProb(val)
		}(float64(i) / 20.0)

		go func() {
			defer wg.Done()
			_ = handler.CorruptProb()
		}()
	}
	wg.Wait()
}
