package manifest_test

import (
	"crypto/sha256"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifest_ComputeID_DeterministicAndZeroID(t *testing.T) {
	var chunkID core.ChunkID
	chunkID[0] = 0x12

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs:   []core.ChunkID{chunkID},
		MerkleRoot: core.Hash(chunkID),
		WholeHash:  core.Hash(chunkID),
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	computedID, err := m.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID failed: %v", err)
	}

	// Calculate manually with zero ID
	mCopy := *m
	mCopy.Descriptor.ID = core.ContentID{}
	data, err := mCopy.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	expectedHash := sha256.Sum256(data)

	if computedID != expectedHash {
		t.Errorf("computed ID %x != expected manual hash %x", computedID, expectedHash)
	}

	// Ensure calling ComputeID did not mutate original manifest's ID
	m.Descriptor.ID = computedID
	secondComputedID, err := m.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID second call failed: %v", err)
	}
	if secondComputedID != computedID {
		t.Errorf("ComputeID non-deterministic: %x vs %x", secondComputedID, computedID)
	}
}

func TestValidateManifestID_ValidAndTampered(t *testing.T) {
	var chunkID core.ChunkID
	chunkID[0] = 0xAA

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 2048,
		},
		ChunkIDs:   []core.ChunkID{chunkID},
		MerkleRoot: core.Hash(chunkID),
		WholeHash:  core.Hash(chunkID),
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	contentID, err := m.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID failed: %v", err)
	}
	m.Descriptor.ID = contentID

	validData, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// 1. Valid manifest validation
	if err := manifest.ValidateManifestID(validData, contentID); err != nil {
		t.Fatalf("ValidateManifestID failed on valid manifest: %v", err)
	}

	// 2. Tampered manifest (chunk ID altered)
	tamperedManifest := *m
	var badChunkID core.ChunkID
	badChunkID[0] = 0xFF
	tamperedManifest.ChunkIDs = []core.ChunkID{badChunkID}
	tamperedData, err := tamperedManifest.Serialize()
	if err != nil {
		t.Fatalf("Serialize tampered manifest failed: %v", err)
	}

	err = manifest.ValidateManifestID(tamperedData, contentID)
	if err == nil {
		t.Fatalf("expected error for tampered manifest, got nil")
	}
	if !errors.Is(err, manifest.ErrIntegrityMismatch) {
		t.Errorf("expected ErrIntegrityMismatch, got %v", err)
	}

	// 3. Mismatched Descriptor.ID inside payload
	mismatchedManifest := *m
	mismatchedManifest.Descriptor.ID[0] ^= 0xFF
	mismatchedData, err := mismatchedManifest.Serialize()
	if err != nil {
		t.Fatalf("Serialize mismatched manifest failed: %v", err)
	}

	err = manifest.ValidateManifestID(mismatchedData, contentID)
	if err == nil {
		t.Fatalf("expected error for mismatched Descriptor.ID, got nil")
	}
	if !errors.Is(err, manifest.ErrIntegrityMismatch) {
		t.Errorf("expected ErrIntegrityMismatch, got %v", err)
	}
}

func TestNilManifest_ComputeID(t *testing.T) {
	var m *manifest.Manifest
	_, err := m.ComputeID()
	if err == nil {
		t.Fatalf("expected error for nil manifest ComputeID")
	}
}
