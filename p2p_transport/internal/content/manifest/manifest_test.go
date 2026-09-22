package manifest_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_Valid(t *testing.T) {
	var id core.ContentID
	id[0] = 0x12

	var chunkID core.ChunkID
	chunkID[0] = 0x34

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   id,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{chunkID},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Version != m.Version {
		t.Errorf("version mismatch: expected %d, got %d", m.Version, deserialized.Version)
	}
	if deserialized.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("content ID mismatch")
	}
	if len(deserialized.ChunkIDs) != 1 || deserialized.ChunkIDs[0] != chunkID {
		t.Errorf("chunk IDs mismatch")
	}
}

func TestDeserialize_PayloadTooLarge(t *testing.T) {
	oversized := make([]byte, manifest.MaxManifestSize+1)
	for i := range oversized {
		oversized[i] = ' '
	}

	_, err := manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_ExceedsChunkLimit(t *testing.T) {
	origLimit := manifest.MaxSupportedChunkCount
	manifest.MaxSupportedChunkCount = 2
	defer func() {
		manifest.MaxSupportedChunkCount = origLimit
	}()

	var id core.ContentID
	id[0] = 0x12

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   id,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01},
			{0x02},
			{0x03},
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if !errors.Is(err, manifest.ErrTooManyChunks) {
		t.Fatalf("expected ErrTooManyChunks, got %v", err)
	}
}

func TestDeserializeFromReader_LimitEnforced(t *testing.T) {
	// Test nil reader
	_, err := manifest.DeserializeFromReader(nil)
	if err == nil {
		t.Fatalf("expected error for nil reader")
	}

	// Test oversized stream
	oversized := make([]byte, manifest.MaxManifestSize+100)
	_, err = manifest.DeserializeFromReader(bytes.NewReader(oversized))
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge for oversized stream, got %v", err)
	}

	// Test valid stream
	var id core.ContentID
	id[0] = 0xAA
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   id,
			Type: manifest.TypeFile,
			Size: 100,
		},
	}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.DeserializeFromReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeserializeFromReader failed for valid stream: %v", err)
	}
	if deserialized.Descriptor.ID != id {
		t.Fatalf("content ID mismatch in stream deserialization")
	}
}
