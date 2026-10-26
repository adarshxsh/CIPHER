package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestFaultInjection_BufferIsolation(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Configure handler with 100% corruption probability
	handler := chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProbability(1.0))
	if handler.CorruptProbability() != 1.0 {
		t.Fatalf("Expected CorruptProbability to be 1.0, got %f", handler.CorruptProbability())
	}

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	chunkID := m.ChunkIDs[0]

	// Read original chunk data from eng1 before transmission
	originalChunk, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}
	originalByte0 := originalChunk.Data[0]

	// Client requests chunk from h1
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Fetch chunk directly
	fetchedChunk, err := client.FetchChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("Expected FetchChunk to fail due to chunk corruption, but succeeded")
	}
	_ = fetchedChunk

	// Verify that eng1's local chunk memory remains UNMUTATED
	eng1ChunkAfter, err := eng1.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk after failed: %v", err)
	}
	if eng1ChunkAfter.Data[0] != originalByte0 {
		t.Fatalf("Local storage chunk buffer was corrupted! Original byte0: 0x%x, Current byte0: 0x%x", originalByte0, eng1ChunkAfter.Data[0])
	}
}

func TestFaultInjection_CustomHook(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	var hookCalled bool
	var hookMu sync.Mutex

	customHook := func(c *core.Chunk) *core.Chunk {
		hookMu.Lock()
		hookCalled = true
		hookMu.Unlock()

		cloned := bytes.Clone(c.Data)
		if len(cloned) > 0 {
			cloned[0] ^= 0xAA
		}
		return &core.Chunk{
			Header: c.Header,
			Data:   cloned,
		}
	}

	handler := chunk.NewStreamHandler(h1, eng1, chunk.WithFaultHook(customHook))
	_ = handler

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	chunkID := m.ChunkIDs[0]

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, _ = client.FetchChunk(ctx, chunkID)

	hookMu.Lock()
	wasCalled := hookCalled
	hookMu.Unlock()

	if !wasCalled {
		t.Fatalf("Expected custom fault hook to be executed")
	}
}

func TestFaultInjection_ConcurrentRace(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1, chunk.WithCorruptProbability(0.5))

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	chunkID := m.ChunkIDs[0]

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id%2 == 0 {
				handler.SetCorruptProbability(0.1 * float64(id%10))
			}
			_ = handler.CorruptProbability()

			client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
			if err != nil {
				return
			}
			defer client.Close()

			_, _ = client.FetchChunk(ctx, chunkID)
		}(i)
	}
	wg.Wait()
}
