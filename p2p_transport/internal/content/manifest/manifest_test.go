package manifest_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifest_SerializeAndDeserialize(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
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

	deserializedStream, err := manifest.DeserializeStream(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeserializeStream failed: %v", err)
	}

	if deserializedStream.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, deserializedStream.Version)
	}
}

func TestManifest_DeserializeOversizedPayload(t *testing.T) {
	oversized := make([]byte, manifest.MaxManifestSize+1)
	_, err := manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestManifest_DeserializeStreamOversizedPayload(t *testing.T) {
	hugeReader := strings.NewReader(strings.Repeat("a", manifest.MaxManifestSize+100))
	_, err := manifest.DeserializeStream(hugeReader)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestManifest_DeserializeStreamNilReader(t *testing.T) {
	_, err := manifest.DeserializeStream(nil)
	if err == nil {
		t.Fatal("expected error for nil reader, got nil")
	}
}
