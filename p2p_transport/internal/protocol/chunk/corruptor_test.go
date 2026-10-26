//go:build fault_injection || !fault_injection

package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	mathrand "math/rand"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

// CorruptingEngine wraps a ChunkEngine and injects chunk corruption for fault testing.
type CorruptingEngine struct {
	chunk.ChunkEngine
	CorruptProb float64
}

func (c *CorruptingEngine) GetChunk(ctx context.Context, id core.ChunkID) (*core.Chunk, error) {
	chunkData, err := c.ChunkEngine.GetChunk(ctx, id)
	if err != nil {
		return nil, err
	}
	if c.CorruptProb > 0 && mathrand.Float64() < c.CorruptProb && len(chunkData.Data) > 0 {
		// Clone chunk data to prevent mutating the underlying storage engine
		corrupted := *chunkData
		corrupted.Data = bytes.Clone(chunkData.Data)
		corrupted.Data[0] ^= 0xFF
		return &corrupted, nil
	}
	return chunkData, nil
}

func TestChunkProtocol_CorruptedChunkRejected(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Wrap eng1 with CorruptingEngine with 100% corruption probability
	corruptEngine := &CorruptingEngine{
		ChunkEngine: eng1,
		CorruptProb: 1.0,
	}

	// Setup Handler on Peer 1 with corruptEngine
	chunk.NewStreamHandler(h1, corruptEngine)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	dataSize := 256 * 1024
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

	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Failed to resolve manifest: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	// Download should fail due to chunk corruption
	err = client.Download(ctx, m2.ChunkIDs)
	if err == nil {
		t.Fatalf("Expected download to fail when chunks are corrupted, but it succeeded")
	}

	// Verify that the underlying storage in eng1 remained untouched/uncorrupted
	originalChunk, err := eng1.GetChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("Failed to get original chunk from engine: %v", err)
	}
	// Re-verify hash of original chunk
	dig := verifier.NewSHA256Digest()
	hasher := dig.Sum(originalChunk.Data)
	if hasher != core.Hash(m.ChunkIDs[0]) {
		t.Fatalf("Original chunk in storage engine was corrupted by test hook!")
	}
}
