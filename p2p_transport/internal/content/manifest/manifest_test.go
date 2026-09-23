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
			ID:   core.ContentID{0x01},
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{{0x02}},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("m.Serialize() failed: %v", err)
	}

	m2, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("manifest.Deserialize() failed: %v", err)
	}

	if m2.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, m2.Version)
	}
	if m2.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("expected descriptor ID %v, got %v", m.Descriptor.ID, m2.Descriptor.ID)
	}
}

func TestDeserialize_OversizedPayload(t *testing.T) {
	data := make([]byte, manifest.MaxManifestSize+1)
	for i := range data {
		data[i] = ' '
	}

	_, err := manifest.Deserialize(data)
	if err == nil {
		t.Fatal("expected error for oversized manifest data, got nil")
	}
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_UnknownFields(t *testing.T) {
	jsonWithExtra := `{"version": 1, "unknown_field": "disallowed"}`
	_, err := manifest.Deserialize([]byte(jsonWithExtra))
	if err == nil {
		t.Fatal("expected error for JSON with unknown fields, got nil")
	}
}

func TestDeserialize_TruncatedJSON(t *testing.T) {
	truncated := `{"version": 1, "descriptor": `
	_, err := manifest.Deserialize([]byte(truncated))
	if err == nil {
		t.Fatal("expected error for truncated JSON, got nil")
	}
}
