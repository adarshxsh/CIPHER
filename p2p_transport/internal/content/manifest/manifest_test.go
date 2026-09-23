package manifest

import (
	"errors"
	"strings"
	"testing"

	"cipher/internal/content/core"
)

func TestDeserializeValidManifest(t *testing.T) {
	m := &Manifest{
		Version: 1,
		Descriptor: ContentDescriptor{
			Type: TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 2, 3},
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	if len(data) > MaxManifestPayloadSize {
		t.Fatalf("Test manifest serialized size (%d) exceeds limit (%d)", len(data), MaxManifestPayloadSize)
	}

	deserialized, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, deserialized.Version)
	}
}

func TestDeserializeOversizedManifest(t *testing.T) {
	oversizedData := []byte(strings.Repeat("a", MaxManifestPayloadSize+1))

	_, err := Deserialize(oversizedData)
	if err == nil {
		t.Fatalf("expected error for oversized manifest payload, got nil")
	}

	if !errors.Is(err, ErrManifestTooLarge) {
		t.Errorf("expected error %v, got %v", ErrManifestTooLarge, err)
	}
}
