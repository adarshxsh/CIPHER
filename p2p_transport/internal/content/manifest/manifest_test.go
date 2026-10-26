package manifest_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidManifest(t *testing.T) {
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

	parsed, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if parsed.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, parsed.Version)
	}
}

func TestDeserialize_OversizedPayload(t *testing.T) {
	oversizedData := make([]byte, manifest.MaxManifestSize+1)

	_, err := manifest.Deserialize(oversizedData)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserializeReader_OversizedReader(t *testing.T) {
	oversizedReader := bytes.NewReader(make([]byte, manifest.MaxManifestSize+100))

	_, err := manifest.DeserializeReader(oversizedReader)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_ZeroAllocationsForOversized(t *testing.T) {
	oversizedData := make([]byte, manifest.MaxManifestSize+10)

	allocs := testing.AllocsPerRun(10, func() {
		_, _ = manifest.Deserialize(oversizedData)
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocations for oversized manifest check, got %f", allocs)
	}
}
