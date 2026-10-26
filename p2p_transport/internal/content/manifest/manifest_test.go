package manifest_test

import (
	"errors"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidManifest(t *testing.T) {
	mOriginal := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x01, 0x02},
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0xAA, 0xBB},
		},
		MerkleRoot: core.Hash{0x11},
		WholeHash:  core.Hash{0x22},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "XChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 24,
			KeyID:          "key123",
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
	if mParsed.Descriptor.ID != mOriginal.Descriptor.ID {
		t.Errorf("expected descriptor ID %v, got %v", mOriginal.Descriptor.ID, mParsed.Descriptor.ID)
	}
	if len(mParsed.ChunkIDs) != 1 {
		t.Errorf("expected 1 chunk ID, got %d", len(mParsed.ChunkIDs))
	}
}

func TestDeserialize_OversizedPayload(t *testing.T) {
	oversized := make([]byte, manifest.MaxManifestSize+1)

	_, err := manifest.Deserialize(oversized)
	if err == nil {
		t.Fatalf("expected error for payload exceeding MaxManifestSize, got nil")
	}
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge error, got: %v", err)
	}
}

func TestDeserialize_TrailingTokens(t *testing.T) {
	mOriginal := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 100,
		},
	}
	validJSON, err := mOriginal.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Case 1: Trailing valid JSON value (e.g., extra JSON object)
	trailingJSONObject := append(validJSON, []byte(`{"extra": true}`)...)
	_, err = manifest.Deserialize(trailingJSONObject)
	if err == nil {
		t.Fatalf("expected error for manifest with trailing JSON object, got nil")
	}
	if !errors.Is(err, manifest.ErrTrailingData) {
		t.Errorf("expected ErrTrailingData for trailing JSON object, got: %v", err)
	}

	// Case 2: Trailing invalid JSON token (garbage text)
	trailingGarbage := append(validJSON, []byte(` invalid_garbage_tokens`)...)
	_, err = manifest.Deserialize(trailingGarbage)
	if err == nil {
		t.Fatalf("expected error for manifest with trailing garbage, got nil")
	}
	if !strings.Contains(err.Error(), "trailing") {
		t.Errorf("expected trailing error, got: %v", err)
	}
}

func TestDeserialize_TrailingWhitespaceAllowed(t *testing.T) {
	mOriginal := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 200,
		},
	}
	validJSON, err := mOriginal.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	withWhitespace := append(validJSON, []byte("  \n\t  ")...)
	mParsed, err := manifest.Deserialize(withWhitespace)
	if err != nil {
		t.Fatalf("Deserialize failed for payload with trailing whitespace: %v", err)
	}
	if mParsed.Version != 1 {
		t.Errorf("expected version 1, got %d", mParsed.Version)
	}
}
