package manifest_test

import (
	"crypto/sha256"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifest_ComputeDigest(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x01, 0x02, 0x03},
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0xAA},
		},
		MerkleRoot: core.Hash{0xBB},
		WholeHash:  core.Hash{0xBB},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	digest1, err := m.ComputeDigest()
	if err != nil {
		t.Fatalf("ComputeDigest failed: %v", err)
	}

	// Change m.Descriptor.ID to something else
	m.Descriptor.ID = core.ContentID{0xFF, 0xEE, 0xDD}

	digest2, err := m.ComputeDigest()
	if err != nil {
		t.Fatalf("ComputeDigest failed: %v", err)
	}

	if digest1 != digest2 {
		t.Fatalf("ComputeDigest should be invariant to m.Descriptor.ID: %x != %x", digest1, digest2)
	}

	// Manually compute expected digest
	canonicalBytes, err := m.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes failed: %v", err)
	}

	expectedHash := sha256.Sum256(canonicalBytes)
	var expectedID core.ContentID
	copy(expectedID[:], expectedHash[:])

	if digest1 != expectedID {
		t.Fatalf("Computed digest %x != expected %x", digest1, expectedID)
	}
}
