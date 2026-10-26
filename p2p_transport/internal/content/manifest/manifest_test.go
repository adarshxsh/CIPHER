package manifest_test

import (
	"bytes"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifestDigest_DeterminismAndZeroedID(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01},
			{0x02},
		},
		MerkleRoot: core.Hash{0xAA},
		WholeHash:  core.Hash{0xAA},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	// Calculate digest when Descriptor.ID is zeroed
	digest1, err := m1.Digest()
	if err != nil {
		t.Fatalf("Digest failed: %v", err)
	}

	if digest1 == (core.ContentID{}) {
		t.Fatal("Expected non-zero digest")
	}

	// Set Descriptor.ID to arbitrary bytes
	var arbitraryID core.ContentID
	arbitraryID[0] = 0x99
	arbitraryID[31] = 0xFF
	m1.Descriptor.ID = arbitraryID

	// Digest should remain identical regardless of Descriptor.ID value
	digest2, err := m1.Digest()
	if err != nil {
		t.Fatalf("Digest failed: %v", err)
	}

	if digest1 != digest2 {
		t.Errorf("Digest mismatch after setting Descriptor.ID: got %x, expected %x", digest2, digest1)
	}

	// Verify Descriptor.ID was preserved after calling Digest
	if m1.Descriptor.ID != arbitraryID {
		t.Errorf("Descriptor.ID mutated: got %x, expected %x", m1.Descriptor.ID, arbitraryID)
	}
}

func TestManifestDigest_ContentSensitivity(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{{0x01}},
	}

	m2 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1025, // modified size
		},
		ChunkIDs: []core.ChunkID{{0x01}},
	}

	d1, err := m1.Digest()
	if err != nil {
		t.Fatalf("m1.Digest failed: %v", err)
	}

	d2, err := m2.Digest()
	if err != nil {
		t.Fatalf("m2.Digest failed: %v", err)
	}

	if bytes.Equal(d1[:], d2[:]) {
		t.Error("Expected different digests for modified manifest fields")
	}
}

func TestManifestDigest_Nil(t *testing.T) {
	var m *manifest.Manifest
	_, err := m.Digest()
	if err == nil {
		t.Error("Expected error calling Digest on nil manifest")
	}
}
