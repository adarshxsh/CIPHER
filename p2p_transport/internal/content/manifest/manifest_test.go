package manifest_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/manifest"
)

func TestDeserialize_Empty(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"nil slice", nil},
		{"empty slice", []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := manifest.Deserialize(tt.data)
			if m != nil {
				t.Errorf("expected nil manifest, got %v", m)
			}
			if !errors.Is(err, manifest.ErrManifestEmpty) {
				t.Errorf("expected ErrManifestEmpty, got %v", err)
			}
		})
	}
}

func TestDeserialize_Oversized(t *testing.T) {
	// Create payload larger than MaxManifestJSONSize (2097117 + 1)
	oversized := make([]byte, manifest.MaxManifestJSONSize+1)
	for i := range oversized {
		oversized[i] = ' '
	}

	m, err := manifest.Deserialize(oversized)
	if m != nil {
		t.Errorf("expected nil manifest, got %v", m)
	}
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Errorf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_Valid(t *testing.T) {
	orig := &manifest.Manifest{
		Version: 1,
	}
	data, err := orig.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if deserialized.Version != orig.Version {
		t.Errorf("expected version %d, got %d", orig.Version, deserialized.Version)
	}
}

func TestDeserialize_BoundaryPadding(t *testing.T) {
	orig := &manifest.Manifest{
		Version: 1,
	}
	data, err := orig.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Pad with spaces up to MaxManifestJSONSize
	paddingNeeded := manifest.MaxManifestJSONSize - len(data)
	if paddingNeeded > 0 {
		paddedData := append(data, bytes.Repeat([]byte(" "), paddingNeeded)...)
		if len(paddedData) != manifest.MaxManifestJSONSize {
			t.Fatalf("expected padded length %d, got %d", manifest.MaxManifestJSONSize, len(paddedData))
		}

		deserialized, err := manifest.Deserialize(paddedData)
		if err != nil {
			t.Fatalf("Deserialize of boundary-sized manifest failed: %v", err)
		}
		if deserialized.Version != orig.Version {
			t.Errorf("expected version %d, got %d", orig.Version, deserialized.Version)
		}
	}
}
