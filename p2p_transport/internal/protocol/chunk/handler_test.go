package chunk_test

import (
	"bytes"
	"context"
	"math/rand"
	"sync"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamHandler_FaultInjection_IsolationAndRace(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Initialize StreamHandler on Peer 1 with 100% corruption probability
	handler1 := chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProb(1.0))
	chunk.NewStreamHandler(h2, eng2)

	if handler1.CorruptProb() != 1.0 {
		t.Fatalf("Expected CorruptProb 1.0, got %f", handler1.CorruptProb())
	}

	ctx := context.Background()
	originalData := make([]byte, 64*1024)
	rand.Read(originalData)

	m, err := eng1.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	chunkID := m.ChunkIDs[0]

	// Verify original chunk byte 0 before requests
	chunkBefore, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get chunk before testing: %v", err)
	}
	byte0Before := chunkBefore.Data[0]

	// Run concurrent requests from peer 2 to peer 1
	const numWorkers = 10
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
			if err != nil {
				t.Errorf("Failed to create client: %v", err)
				return
			}
			defer client.Close()

			// Concurrently update corrupt prob dynamically to test thread safety
			handler1.SetCorruptProb(1.0)

			// Download chunk which will receive corrupted byte
			_ = client.Download(ctx, []core.ChunkID{chunkID})
		}()
	}

	wg.Wait()

	// Verify original chunk in store on Peer 1 was NOT mutated
	chunkAfter, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("Failed to get chunk after testing: %v", err)
	}

	if chunkAfter.Data[0] != byte0Before {
		t.Fatalf("Storage corruption detected! Original byte 0 changed from %x to %x", byte0Before, chunkAfter.Data[0])
	}
}

func TestStreamHandler_OptionsAndConfig(t *testing.T) {
	h1, _ := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Test default initialization
	hDefault := chunk.NewStreamHandler(h1, eng1)
	if hDefault.CorruptProb() != 0.0 {
		t.Errorf("Expected default CorruptProb 0.0, got %f", hDefault.CorruptProb())
	}

	// Test WithConfig option
	cfg := chunk.HandlerConfig{CorruptProb: 0.5}
	hConfig := chunk.NewStreamHandler(h1, eng1, chunk.WithConfig(cfg))
	if hConfig.CorruptProb() != 0.5 {
		t.Errorf("Expected CorruptProb 0.5, got %f", hConfig.CorruptProb())
	}

	// Test bounds clamping
	hConfig.SetCorruptProb(-0.5)
	if hConfig.CorruptProb() != 0.0 {
		t.Errorf("Expected clamped CorruptProb 0.0, got %f", hConfig.CorruptProb())
	}

	hConfig.SetCorruptProb(1.5)
	if hConfig.CorruptProb() != 1.0 {
		t.Errorf("Expected clamped CorruptProb 1.0, got %f", hConfig.CorruptProb())
	}
}
