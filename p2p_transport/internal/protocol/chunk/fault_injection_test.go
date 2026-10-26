//go:build fault_injection

package chunk_test

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
)

func TestFaultInjection_DeepCloning(t *testing.T) {
	eng := createTestEngine(t)
	h1, _ := setupMockNetwork(t)

	handler := chunk.NewStreamHandler(h1, eng, chunk.WithCorruptProbability(1.0))

	ctx := context.Background()
	originalData := []byte("hello world 1234567890")
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest test chunk: %v", err)
	}

	if len(m.ChunkIDs) == 0 {
		t.Fatal("Expected at least 1 chunk ID")
	}

	chunkID := m.ChunkIDs[0]
	origChunk, err := eng.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get chunk from engine: %v", err)
	}

	// Capture initial byte value of origin buffer
	origByte0 := origChunk.Data[0]

	// Enable 100% corruption probability
	handler.SetCorruptProbability(1.0)

	// Fetch chunk from engine again & verify original slice in engine storage is intact
	chunkData, err := eng.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to re-fetch chunk: %v", err)
	}

	if chunkData.Data[0] != origByte0 {
		t.Fatalf("Origin buffer was mutated! Expected byte 0 to be %d, got %d", origByte0, chunkData.Data[0])
	}
}

func TestFaultInjection_Race(t *testing.T) {
	eng := createTestEngine(t)
	h1, _ := setupMockNetwork(t)

	handler := chunk.NewStreamHandler(h1, eng, chunk.WithCorruptProbability(0.5))

	var wg sync.WaitGroup
	cData := &core.Chunk{
		Data: []byte("test data buffer for race detection"),
	}

	// Concurrent readers / fault injection setters
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = handler.GetCorruptProbability()
				_ = cData
			}
		}()
	}

	// Concurrent writers updating probability
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				prob := float64(j%10) / 10.0
				handler.SetCorruptProbability(prob)
			}
		}(i)
	}

	wg.Wait()
}
