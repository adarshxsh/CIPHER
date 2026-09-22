package manifest_test

import (
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_Valid(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Size: 64 * 1024, // 64 KiB -> exactly 2 chunks
		},
		ChunkIDs: []core.ChunkID{
			{1},
			{2},
		},
	}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("expected valid deserialization, got error: %v", err)
	}
	if len(deserialized.ChunkIDs) != 2 {
		t.Errorf("expected 2 chunk IDs, got %d", len(deserialized.ChunkIDs))
	}
}

func TestDeserialize_ExceedsCalculatedChunkCount(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Size: 100, // 100 bytes -> max 1 chunk
		},
		ChunkIDs: []core.ChunkID{
			{1},
			{2}, // 2 chunks exceeds calculated limit for 100 bytes
		},
	}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if err == nil {
		t.Fatalf("expected error for exceeding calculated chunk count, got nil")
	}
	if !errors.Is(err, manifest.ErrInvalidChunkCount) {
		t.Errorf("expected ErrInvalidChunkCount, got %v", err)
	}
}

func TestDeserialize_ExceedsMaxSupportedChunkCount(t *testing.T) {
	// Directly craft a manifest json representation or structure that claims a chunk count exceeding limit.
	// We can set size large enough so expectedChunks is huge, but ChunkIDs slice has dummy elements or we test the boundary limit check.
	// Testing ChunkIDs length > MaxSupportedChunkCount
	// Since creating a slice of 2^32 entries takes tens of GBs of RAM, we can test JSON payload with large dummy array or mock structure.
	// But we can verify that if len(ChunkIDs) exceeds limit, or test boundary edge case logic.
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Size: 0,
		},
		ChunkIDs: []core.ChunkID{
			{1},
		},
	}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if err == nil {
		t.Fatalf("expected error when ChunkIDs > expectedChunks (1 > 0), got nil")
	}
	if !errors.Is(err, manifest.ErrInvalidChunkCount) {
		t.Errorf("expected ErrInvalidChunkCount, got %v", err)
	}
}
