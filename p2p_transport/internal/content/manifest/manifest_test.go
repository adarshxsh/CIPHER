package manifest_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestCanonicalBytes_ZeroesDescriptorIDAndIsDeterministic(t *testing.T) {
	var originalID core.ContentID
	originalID[0] = 0xDE
	originalID[31] = 0xAD

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   originalID,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 2, 3},
			{4, 5, 6},
		},
		MerkleRoot: core.Hash{9, 9, 9},
		WholeHash:  core.Hash{9, 9, 9},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	b1, err := m.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes failed: %v", err)
	}

	// Ensure original manifest ID was not mutated
	if m.Descriptor.ID != originalID {
		t.Fatalf("m.Descriptor.ID was mutated, expected %x got %x", originalID, m.Descriptor.ID)
	}

	// Verify Descriptor.ID in serialized canonical JSON is zeroed out
	var unmarshaled manifest.Manifest
	if err := json.Unmarshal(b1, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal of CanonicalBytes failed: %v", err)
	}

	var zeroID core.ContentID
	if unmarshaled.Descriptor.ID != zeroID {
		t.Fatalf("expected zeroed Descriptor.ID in canonical bytes, got %x", unmarshaled.Descriptor.ID)
	}

	// Test determinism
	b2, err := m.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes second call failed: %v", err)
	}

	if !bytes.Equal(b1, b2) {
		t.Fatalf("CanonicalBytes is non-deterministic")
	}
}

func TestComputeContentID(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 2048,
		},
	}

	cid, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	canonicalBytes, err := m.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes failed: %v", err)
	}

	expectedHash := sha256.Sum256(canonicalBytes)
	var expectedID core.ContentID
	copy(expectedID[:], expectedHash[:])

	if cid != expectedID {
		t.Fatalf("ComputeContentID mismatch: expected %x, got %x", expectedID, cid)
	}
}
