package manifest_test

import (
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_SizeLimits(t *testing.T) {
	// Empty slice
	_, err := manifest.Deserialize([]byte{})
	if !errors.Is(err, manifest.ErrInvalidManifest) {
		t.Fatalf("expected ErrInvalidManifest for empty slice, got %v", err)
	}

	// Oversized slice
	oversized := make([]byte, manifest.MaxManifestSize+1)
	_, err = manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrInvalidManifest) {
		t.Fatalf("expected ErrInvalidManifest for oversized slice, got %v", err)
	}

	// Valid manifest JSON
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x01},
			Type: manifest.TypeFile,
			Size: 100,
		},
	}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed for valid manifest: %v", err)
	}
	if deserialized.Version != m.Version {
		t.Fatalf("expected version %d, got %d", m.Version, deserialized.Version)
	}
}
