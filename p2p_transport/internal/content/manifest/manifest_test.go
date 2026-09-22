package manifest_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidManifest(t *testing.T) {
	mOriginal := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01, 0x02},
			{0x03, 0x04},
		},
	}

	data, err := mOriginal.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	mParsed, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if mParsed.Version != mOriginal.Version {
		t.Errorf("expected version %d, got %d", mOriginal.Version, mParsed.Version)
	}
	if len(mParsed.ChunkIDs) != len(mOriginal.ChunkIDs) {
		t.Errorf("expected %d chunk IDs, got %d", len(mOriginal.ChunkIDs), len(mParsed.ChunkIDs))
	}
}

func TestDeserialize_RejectsOversizedData(t *testing.T) {
	// Payload strictly larger than MaxManifestSize
	oversized := make([]byte, manifest.MaxManifestSize+1)

	_, err := manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge for Deserialize, got %v", err)
	}

	_, err = manifest.DeserializeReader(bytes.NewReader(oversized))
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge for DeserializeReader, got %v", err)
	}
}

func TestDeserializeReader_ValidManifest(t *testing.T) {
	mOriginal := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 512,
		},
	}

	data, err := mOriginal.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	mParsed, err := manifest.DeserializeReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeserializeReader failed: %v", err)
	}

	if mParsed.Descriptor.Size != 512 {
		t.Errorf("expected size 512, got %d", mParsed.Descriptor.Size)
	}
}
