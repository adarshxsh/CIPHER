package manifest

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"cipher/internal/content/core"
)

func TestManifest_ComputeContentID_Deterministic(t *testing.T) {
	m := &Manifest{
		Version: 1,
		Descriptor: ContentDescriptor{
			Type: TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 2, 3},
		},
	}

	id1, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	expectedHash := sha256.Sum256(mBytes)
	if !bytes.Equal(id1[:], expectedHash[:]) {
		t.Fatalf("ComputeContentID %x != sha256(Serialize) %x", id1, expectedHash)
	}

	// Setting m.Descriptor.ID to id1 must not alter m.Serialize() or ComputeContentID() output
	m.Descriptor.ID = id1

	id2, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed after ID set: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("ComputeContentID changed after setting ID: %x != %x", id1, id2)
	}

	mBytes2, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed after ID set: %v", err)
	}

	if !bytes.Equal(mBytes, mBytes2) {
		t.Fatalf("Serialized bytes changed after setting Descriptor.ID")
	}
}
