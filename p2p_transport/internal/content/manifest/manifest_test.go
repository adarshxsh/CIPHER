package manifest_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestComputeContentID_Deterministic(t *testing.T) {
	m := &manifest.Manifest{
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

	cid1, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	// Setting m.Descriptor.ID to a non-zero value should NOT alter the computed ContentID
	m.Descriptor.ID = cid1

	cid2, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed on second call: %v", err)
	}

	if cid1 != cid2 {
		t.Errorf("computed ContentIDs do not match: %x vs %x", cid1, cid2)
	}

	// Verify SHA-256 calculation matches manual calculation with zeroed Descriptor.ID
	copyM := *m
	copyM.Descriptor.ID = core.ContentID{}
	data, err := json.Marshal(&copyM)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	expectedHash := sha256.Sum256(data)
	var expectedCID core.ContentID
	copy(expectedCID[:], expectedHash[:])

	if cid1 != expectedCID {
		t.Errorf("ComputeContentID %x != expected SHA-256 digest %x", cid1, expectedCID)
	}
}

func TestComputeContentID_DetectsTampering(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 2048,
		},
		ChunkIDs: []core.ChunkID{{10, 20, 30}},
	}

	cidOriginal, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	// Tamper with size
	mTampered := *m
	mTampered.Descriptor.Size = 9999

	cidTampered, err := mTampered.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed for tampered manifest: %v", err)
	}

	if cidOriginal == cidTampered {
		t.Errorf("Expected different ContentIDs for tampered manifest, got identical %x", cidOriginal)
	}
}

func TestComputeContentID_NilManifest(t *testing.T) {
	var m *manifest.Manifest
	_, err := m.ComputeContentID()
	if err == nil {
		t.Errorf("Expected error for nil manifest, got nil")
	}
}

func TestErrContentIDMismatch_Is(t *testing.T) {
	err := manifest.ErrContentIDMismatch
	if !errors.Is(err, manifest.ErrContentIDMismatch) {
		t.Errorf("errors.Is failed for ErrContentIDMismatch")
	}
}
