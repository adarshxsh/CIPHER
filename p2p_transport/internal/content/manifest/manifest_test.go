package manifest_test

import (
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidManifest(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, deserialized.Version)
	}
	if deserialized.Descriptor.Size != m.Descriptor.Size {
		t.Errorf("expected size %d, got %d", m.Descriptor.Size, deserialized.Descriptor.Size)
	}
}

func TestDeserialize_OversizedManifest(t *testing.T) {
	// Payload larger than manifest.MaxManifestSize
	oversized := make([]byte, manifest.MaxManifestSize+100)
	for i := range oversized {
		oversized[i] = ' '
	}

	_, err := manifest.Deserialize(oversized)
	if err == nil {
		t.Fatal("expected error when deserializing oversized manifest payload, got nil")
	}
}

func TestDeserialize_TruncatedJSON(t *testing.T) {
	// Construct a large JSON string that gets truncated by MaxManifestSize limit
	var sb strings.Builder
	sb.WriteString(`{"version":1,"descriptor":{"size":100},"crypto":{"key_id":"`)
	sb.WriteString(strings.Repeat("a", manifest.MaxManifestSize))
	sb.WriteString(`"}}`)

	_, err := manifest.Deserialize([]byte(sb.String()))
	if err == nil {
		t.Fatal("expected error for manifest payload exceeding limit, got nil")
	}
}
