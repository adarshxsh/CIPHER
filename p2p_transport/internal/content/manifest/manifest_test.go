package manifest_test

import (
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_EmptyInput(t *testing.T) {
	_, err := manifest.Deserialize([]byte{})
	if !errors.Is(err, manifest.ErrEmptyManifest) {
		t.Fatalf("expected ErrEmptyManifest, got %v", err)
	}
}

func TestDeserialize_OversizedInput(t *testing.T) {
	oversized := make([]byte, manifest.MaxManifestSize+1)
	_, err := manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_ValidManifest(t *testing.T) {
	mOriginal := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01, 0x02},
		},
	}

	data, err := mOriginal.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	mDeserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize valid manifest: %v", err)
	}

	if mDeserialized.Version != mOriginal.Version {
		t.Errorf("version mismatch: expected %d, got %d", mOriginal.Version, mDeserialized.Version)
	}
	if len(mDeserialized.ChunkIDs) != len(mOriginal.ChunkIDs) {
		t.Errorf("chunk count mismatch: expected %d, got %d", len(mOriginal.ChunkIDs), len(mDeserialized.ChunkIDs))
	}
}

func TestDeserialize_UnknownFields(t *testing.T) {
	jsonWithUnknown := []byte(`{"version":1,"unknown_field":"value"}`)
	_, err := manifest.Deserialize(jsonWithUnknown)
	if err == nil {
		t.Fatalf("expected error for manifest containing unknown fields, got nil")
	}
}
