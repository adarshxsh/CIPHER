package manifest_test

import (
	"crypto/sha256"
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
		},
		WholeHash: core.Hash{4, 5, 6},
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "ChaCha20-Poly1305",
			Version:   1,
		},
	}

	id1, err := m1.ComputeContentID()
	if err != nil {
		t.Fatalf("m1.ComputeContentID() failed: %v", err)
	}

	m2 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 2, 3},
		},
		WholeHash: core.Hash{4, 5, 6},
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "ChaCha20-Poly1305",
			Version:   1,
		},
	}

	id2, err := m2.ComputeContentID()
	if err != nil {
		t.Fatalf("m2.ComputeContentID() failed: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected deterministic ContentID, got %x vs %x", id1, id2)
	}
}

func TestComputeContentID_ZeroesInternalContentID(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0xAA, 0xBB, 0xCC},
			Type: manifest.TypeFile,
			Size: 2048,
		},
		WholeHash: core.Hash{7, 8, 9},
	}

	idWithNonZero, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	mZero := *m
	mZero.Descriptor.ID = core.ContentID{}
	idWithZero, err := mZero.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	if idWithNonZero != idWithZero {
		t.Errorf("ComputeContentID did not zero internal ID field: %x vs %x", idWithNonZero, idWithZero)
	}

	// Verify exact SHA256 matches canonical JSON with zeroed ID
	mZeroData, _ := mZero.Serialize()
	expectedHash := sha256.Sum256(mZeroData)
	var expectedID core.ContentID
	copy(expectedID[:], expectedHash[:])

	if idWithNonZero != expectedID {
		t.Errorf("derived ContentID %x != expected SHA-256 %x", idWithNonZero, expectedID)
	}
}

func TestComputeContentID_DetectsFieldChanges(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 100,
		},
	}

	id1, _ := m1.ComputeContentID()

	m2 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 200, // Modified size
		},
	}

	id2, _ := m2.ComputeContentID()

	if id1 == id2 {
		t.Errorf("expected different ContentIDs for different manifest fields, but got identical %x", id1)
	}
}
