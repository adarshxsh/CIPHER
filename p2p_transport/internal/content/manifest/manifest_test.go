package manifest_test

import (
	"errors"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidManifest(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{1, 2, 3},
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{{1}, {2}},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(deserialized.ChunkIDs) != 2 {
		t.Fatalf("expected 2 chunk IDs, got %d", len(deserialized.ChunkIDs))
	}
}

func TestDeserialize_RejectsOversizedPayload(t *testing.T) {
	// Create payload slightly larger than MaxManifestSizeBytes (256 KiB)
	oversized := make([]byte, manifest.MaxManifestSizeBytes+1)
	for i := range oversized {
		oversized[i] = ' '
	}

	_, err := manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_RejectsExcessiveChunkIDs(t *testing.T) {
	// Create manifest with more than MaxManifestChunkCount chunk IDs
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: make([]core.ChunkID, manifest.MaxManifestChunkCount+1),
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

func TestDeserialize_RejectsMalformedJSON(t *testing.T) {
	invalidJSON := []byte("{invalid_json_payload}")
	_, err := manifest.Deserialize(invalidJSON)
	if err == nil {
		t.Fatal("expected error deserializing malformed JSON, got nil")
	}
	if !strings.Contains(err.Error(), "invalid character") && !strings.Contains(err.Error(), "looking for") {
		t.Logf("Got expected json error: %v", err)
	}
}
