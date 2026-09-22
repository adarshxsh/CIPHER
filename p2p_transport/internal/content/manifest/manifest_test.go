package manifest_test

import (
	"errors"
	"testing"

	"cipher/internal/content/manifest"
)

func TestDeserialize_EnforcesSizeLimit(t *testing.T) {
	// 1. Valid manifest
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 100,
		},
	}
	m.Descriptor.ID[0] = 0x01

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if deserialized.Version != m.Version {
		t.Errorf("Expected version %d, got %d", m.Version, deserialized.Version)
	}

	// 2. Oversized payload
	oversized := make([]byte, manifest.MaxManifestJSONSize+1)
	_, err = manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("Expected ErrManifestTooLarge, got %v", err)
	}
}
