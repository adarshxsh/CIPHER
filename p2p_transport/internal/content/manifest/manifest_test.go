package manifest_test

import (
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifest_DeriveContentID_Deterministic(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs:   []core.ChunkID{{0x01}, {0x02}},
		MerkleRoot: core.Hash{0xAA},
		WholeHash:  core.Hash{0xAA},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	id1, err := m1.DeriveContentID()
	if err != nil {
		t.Fatalf("DeriveContentID failed: %v", err)
	}

	// Calculate again on identical manifest
	m2 := *m1
	id2, err := m2.DeriveContentID()
	if err != nil {
		t.Fatalf("DeriveContentID failed: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("DeriveContentID expected to be deterministic, got %x vs %x", id1, id2)
	}

	// Verify ID changes when content changes
	m3 := *m1
	m3.ChunkIDs = []core.ChunkID{{0x01}, {0x03}}
	id3, err := m3.DeriveContentID()
	if err != nil {
		t.Fatalf("DeriveContentID failed: %v", err)
	}

	if id1 == id3 {
		t.Fatalf("DeriveContentID should produce different IDs for different chunk IDs")
	}
}

func TestManifest_VerifyIntegrity(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 500,
		},
		ChunkIDs:   []core.ChunkID{{0x10}},
		MerkleRoot: core.Hash{0x20},
		WholeHash:  core.Hash{0x20},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	id, err := m.DeriveContentID()
	if err != nil {
		t.Fatalf("DeriveContentID failed: %v", err)
	}

	m.Descriptor.ID = id
	if !m.VerifyIntegrity() {
		t.Fatalf("VerifyIntegrity should return true for valid ContentID binding")
	}

	// Tamper with ID
	m.Descriptor.ID[0] ^= 0xFF
	if m.VerifyIntegrity() {
		t.Fatalf("VerifyIntegrity should return false when Descriptor.ID is tampered")
	}
}
