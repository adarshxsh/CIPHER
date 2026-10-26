package manifest_test

import (
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
			Size: 100,
		},
		ChunkIDs: []core.ChunkID{{0x01}},
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
}

func TestDeserialize_EmptyPayload(t *testing.T) {
	_, err := manifest.Deserialize([]byte{})
	if !errors.Is(err, manifest.ErrEmptyManifest) {
		t.Fatalf("expected ErrEmptyManifest, got %v", err)
	}
}

func TestDeserialize_OversizedPayload(t *testing.T) {
	oversized := make([]byte, manifest.MaxManifestSizeBytes+1)
	_, err := manifest.Deserialize(oversized)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}
