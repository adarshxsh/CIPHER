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
		},
	}

	digest1, err := m.ComputeDigest()
	if err != nil {
		t.Fatalf("ComputeDigest failed: %v", err)
	}

	if digest1 == (core.ContentID{}) {
		t.Fatalf("expected non-zero digest")
	}

	// Changing m.Descriptor.ID must NOT change ComputeDigest output
	m.Descriptor.ID = core.ContentID{0xFF, 0xEE, 0xDD}
	digest2, err := m.ComputeDigest()
	if err != nil {
		t.Fatalf("ComputeDigest failed: %v", err)
	}

	if digest1 != digest2 {
		t.Fatalf("ComputeDigest should ignore Descriptor.ID field, got %x vs %x", digest1, digest2)
	}

	// Descriptor.ID should be preserved after ComputeDigest returns
	if m.Descriptor.ID != (core.ContentID{0xFF, 0xEE, 0xDD}) {
		t.Fatalf("Descriptor.ID was not restored, got %x", m.Descriptor.ID)
	}

	// CanonicalBytes should serialize with zero ID
	canonical, err := m.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes failed: %v", err)
	}

	expectedHash := sha256.Sum256(canonical)
	if digest1 != core.ContentID(expectedHash) {
		t.Fatalf("digest mismatch: expected %x, got %x", expectedHash, digest1)
	}

	// Modifying other fields MUST change the digest
	m.Descriptor.Size = 2048
	digest3, err := m.ComputeDigest()
	if err != nil {
		t.Fatalf("ComputeDigest failed: %v", err)
	}
	if digest1 == digest3 {
		t.Fatalf("modifying manifest size should change digest")
	}
}
