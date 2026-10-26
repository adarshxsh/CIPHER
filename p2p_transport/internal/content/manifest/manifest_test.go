package manifest_test

import (
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestComputeContentID_Deterministic(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 2, 3},
			{4, 5, 6},
		},
		MerkleRoot: core.Hash{7, 8, 9},
		WholeHash:  core.Hash{7, 8, 9},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	id1, err := m1.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	id2, err := m1.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("expected deterministic ID, got %x and %x", id1, id2)
	}
}

func TestComputeContentID_IgnoresExistingID(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0xFF, 0xEE},
			Type: manifest.TypeFile,
			Size: 2048,
		},
		ChunkIDs: []core.ChunkID{{10, 20}},
	}

	m2 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x00},
			Type: manifest.TypeFile,
			Size: 2048,
		},
		ChunkIDs: []core.ChunkID{{10, 20}},
	}

	id1, err := m1.ComputeContentID()
	if err != nil {
		t.Fatalf("m1 ComputeContentID failed: %v", err)
	}

	id2, err := m2.ComputeContentID()
	if err != nil {
		t.Fatalf("m2 ComputeContentID failed: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("expected same ContentID regardless of Descriptor.ID, got %x vs %x", id1, id2)
	}
}

func TestComputeContentID_DetectsChanges(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 100,
		},
		ChunkIDs: []core.ChunkID{{1}},
	}

	m2 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 101, // modified size
		},
		ChunkIDs: []core.ChunkID{{1}},
	}

	id1, _ := m1.ComputeContentID()
	id2, _ := m2.ComputeContentID()

	if id1 == id2 {
		t.Fatalf("expected different ContentIDs for different manifest fields")
	}
}
