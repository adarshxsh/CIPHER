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
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01, 0x02},
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Version != m.Version || deserialized.Descriptor.Size != m.Descriptor.Size {
		t.Fatalf("Deserialized content mismatch: %+v vs %+v", deserialized, m)
	}
}

func TestDeserialize_Empty(t *testing.T) {
	_, err := manifest.Deserialize([]byte{})
	if err == nil {
		t.Fatal("expected error for empty manifest data, got nil")
	}
	if !errors.Is(err, manifest.ErrEmptyManifest) {
		t.Fatalf("expected ErrEmptyManifest, got: %v", err)
	}
}

func TestDeserialize_Oversized(t *testing.T) {
	oversizedData := make([]byte, manifest.MaxManifestSize+1)
	for i := range oversizedData {
		oversizedData[i] = ' '
	}

	_, err := manifest.Deserialize(oversizedData)
	if err == nil {
		t.Fatal("expected error for oversized manifest data, got nil")
	}
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got: %v", err)
	}
}

func TestDeserialize_InvalidJSON(t *testing.T) {
	invalidJSON := []byte("{invalid json payload")
	_, err := manifest.Deserialize(invalidJSON)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}
