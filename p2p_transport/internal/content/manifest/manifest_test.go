package manifest_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func generateTestChunkIDs(count int) []core.ChunkID {
	chunkIDs := make([]core.ChunkID, count)
	for i := 0; i < count; i++ {
		_, _ = rand.Read(chunkIDs[i][:])
	}
	return chunkIDs
}

func TestManifest_SerializeAndDeserialize_Valid(t *testing.T) {
	chunkIDs := generateTestChunkIDs(5)
	var cid core.ContentID
	_, _ = rand.Read(cid[:])

	// Compute expected Merkle root
	var idConcat []byte
	for _, id := range chunkIDs {
		idConcat = append(idConcat, id[:]...)
	}
	expectedRootHash := sha256.Sum256(idConcat)
	var expectedRoot core.Hash
	copy(expectedRoot[:], expectedRootHash[:])

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 1024 * 160,
		},
		ChunkIDs:   chunkIDs,
		MerkleRoot: expectedRoot,
		WholeHash:  expectedRoot,
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Descriptor.ID != cid {
		t.Errorf("ContentID mismatch: got %x, want %x", deserialized.Descriptor.ID, cid)
	}
	if deserialized.MerkleRoot != expectedRoot {
		t.Errorf("MerkleRoot mismatch: got %x, want %x", deserialized.MerkleRoot, expectedRoot)
	}
	if deserialized.WholeHash != expectedRoot {
		t.Errorf("WholeHash mismatch: got %x, want %x", deserialized.WholeHash, expectedRoot)
	}
}

func TestManifest_Deserialize_RecalculatesMissingRoot(t *testing.T) {
	chunkIDs := generateTestChunkIDs(3)
	var cid core.ContentID
	_, _ = rand.Read(cid[:])

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 1024 * 96,
		},
		ChunkIDs: chunkIDs,
		// MerkleRoot and WholeHash intentionally left zero
	}

	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	expectedRoot := m.CalculateMerkleRoot()
	if deserialized.MerkleRoot != expectedRoot {
		t.Errorf("MerkleRoot not recalculated correctly: got %x, want %x", deserialized.MerkleRoot, expectedRoot)
	}
	if deserialized.WholeHash != expectedRoot {
		t.Errorf("WholeHash not recalculated correctly: got %x, want %x", deserialized.WholeHash, expectedRoot)
	}
}

func TestManifest_Deserialize_TamperedMerkleRoot_Fails(t *testing.T) {
	chunkIDs := generateTestChunkIDs(4)
	var cid core.ContentID
	_, _ = rand.Read(cid[:])

	var fakeRoot core.Hash
	_, _ = rand.Read(fakeRoot[:])

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 1024 * 128,
		},
		ChunkIDs:   chunkIDs,
		MerkleRoot: fakeRoot, // Corrupted root
		WholeHash:  fakeRoot,
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if err == nil {
		t.Fatalf("Expected error for tampered MerkleRoot, got nil")
	}
	if err != manifest.ErrMerkleRootMismatch {
		t.Errorf("Unexpected error: %v, want %v", err, manifest.ErrMerkleRootMismatch)
	}
}

func TestManifest_Deserialize_TamperedChunkIDs_Fails(t *testing.T) {
	chunkIDs := generateTestChunkIDs(4)
	var cid core.ContentID
	_, _ = rand.Read(cid[:])

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 1024 * 128,
		},
		ChunkIDs: chunkIDs,
	}
	m.MerkleRoot = m.CalculateMerkleRoot()
	m.WholeHash = m.CalculateWholeHash()

	// Tamper with chunk IDs in manifest struct and attempt deserialize with old MerkleRoot
	m.ChunkIDs[0][0] ^= 0xFF
	m.MerkleRoot = m.CalculateMerkleRoot() // new root, but in raw JSON we put old root
	// Let's modify the JSON string directly:
	m.ChunkIDs[0] = chunkIDs[0] // restore chunkIDs
	rawJSON, _ := m.Serialize() // rawJSON has original MerkleRoot
	// Now corrupt a chunkID in raw JSON
	tamperedJSON := bytes.Replace(rawJSON, []byte(fmt.Sprintf("%x", chunkIDs[0])), []byte(fmt.Sprintf("%x", m.ChunkIDs[0])), 1)

	_, err := manifest.Deserialize(tamperedJSON)
	if err == nil {
		t.Fatalf("Expected error for tampered chunk IDs, got nil")
	}
}

func TestManifest_Deserialize_EmptyData(t *testing.T) {
	_, err := manifest.Deserialize(nil)
	if err != manifest.ErrEmptyManifest {
		t.Errorf("Expected ErrEmptyManifest, got %v", err)
	}

	_, err = manifest.Deserialize([]byte{})
	if err != manifest.ErrEmptyManifest {
		t.Errorf("Expected ErrEmptyManifest, got %v", err)
	}
}

func TestManifest_Deserialize_OversizedData(t *testing.T) {
	hugeData := make([]byte, manifest.MaxManifestJSONSize+1)
	_, err := manifest.Deserialize(hugeData)
	if err != manifest.ErrManifestTooLarge {
		t.Errorf("Expected ErrManifestTooLarge, got %v", err)
	}
}

func TestManifest_ComputeContentID_Deterministic(t *testing.T) {
	chunkIDs := generateTestChunkIDs(2)
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 64000,
		},
		ChunkIDs: chunkIDs,
	}
	m.MerkleRoot = m.CalculateMerkleRoot()

	cid1, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	cid2, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}

	if cid1 != cid2 {
		t.Errorf("ComputeContentID is not deterministic: %x vs %x", cid1, cid2)
	}

	if cid1 == (core.ContentID{}) {
		t.Errorf("ComputeContentID returned zero ContentID")
	}
}

func TestManifest_Deserialize_InvalidJSON(t *testing.T) {
	badJSON := []byte(`{"version": 1, "descriptor": {`)
	_, err := manifest.Deserialize(badJSON)
	if err == nil {
		t.Fatalf("Expected error for malformed JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode manifest JSON") {
		t.Errorf("Unexpected error message: %v", err)
	}
}
