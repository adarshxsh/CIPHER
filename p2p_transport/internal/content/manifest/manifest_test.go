package manifest_test

import (
	"errors"
	"testing"

	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidAndBoundaries(t *testing.T) {
	// 1. Valid manifest serialization & deserialization
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
	}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed for valid data: %v", err)
	}
	if deserialized.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, deserialized.Version)
	}

	// 2. Empty payload rejection prior to unmarshaling
	_, err = manifest.Deserialize([]byte{})
	if !errors.Is(err, manifest.ErrManifestDataEmpty) {
		t.Fatalf("expected ErrManifestDataEmpty for empty data, got %v", err)
	}

	// 3. Oversized payload rejection prior to unmarshaling
	oversizedData := make([]byte, manifest.MaxManifestSize+1)
	_, err = manifest.Deserialize(oversizedData)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge for data exceeding MaxManifestSize (%d bytes), got %v", manifest.MaxManifestSize, err)
	}
}
