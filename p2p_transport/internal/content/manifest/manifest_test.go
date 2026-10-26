package manifest_test

import (
	"bytes"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidManifest(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{1, 2, 3},
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
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, deserialized.Version)
	}
	if deserialized.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("expected ContentID %x, got %x", m.Descriptor.ID, deserialized.Descriptor.ID)
	}
}

func TestDeserialize_RejectsOversizedBytes(t *testing.T) {
	// Create payload larger than MaxManifestSize
	oversized := make([]byte, manifest.MaxManifestSize+100)

	_, err := manifest.Deserialize(oversized)
	if err == nil {
		t.Fatal("expected error for oversized byte slice, got nil")
	}
	if err != manifest.ErrManifestTooLarge {
		t.Errorf("expected ErrManifestTooLarge, got: %v", err)
	}
}

type infiniteReader struct{}

func (r *infiniteReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 'a'
	}
	return len(p), nil
}

func TestDeserializeFromStream_ValidStream(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{4, 5, 6},
			Type: manifest.TypeFile,
			Size: 2048,
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.DeserializeFromStream(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeserializeFromStream failed: %v", err)
	}

	if deserialized.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("expected ContentID %x, got %x", m.Descriptor.ID, deserialized.Descriptor.ID)
	}
}

func TestDeserializeFromStream_RejectsOversizedStreamWithoutExhaustion(t *testing.T) {
	// Use an infinite reader stream that would consume infinite memory if unconstrained
	inf := &infiniteReader{}

	_, err := manifest.DeserializeFromStream(inf)
	if err == nil {
		t.Fatal("expected error when reading oversized stream, got nil")
	}
	if err != manifest.ErrManifestTooLarge {
		t.Errorf("expected ErrManifestTooLarge, got: %v", err)
	}
}

func TestDeserializeFromStream_NilReader(t *testing.T) {
	_, err := manifest.DeserializeFromStream(nil)
	if err == nil {
		t.Fatal("expected error for nil reader, got nil")
	}
}
