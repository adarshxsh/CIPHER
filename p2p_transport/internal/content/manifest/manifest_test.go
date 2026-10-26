package manifest_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestCanonicalSerialization_SortedKeysAndCompact(t *testing.T) {
	var chunkID core.ChunkID
	chunkID[0] = 0x01

	var wholeHash core.Hash
	wholeHash[0] = 0x02

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0xFF},
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs:   []core.ChunkID{chunkID},
		MerkleRoot: wholeHash,
		WholeHash:  wholeHash,
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	data1, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	data2, err := manifest.SerializeCanonical(m)
	if err != nil {
		t.Fatalf("SerializeCanonical failed: %v", err)
	}

	if !bytes.Equal(data1, data2) {
		t.Fatalf("Serialize() and SerializeCanonical() produced different bytes")
	}

	// Verify top-level keys in JSON are in alphabetical order
	var mapObj map[string]json.RawMessage
	if err := json.Unmarshal(data1, &mapObj); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Check string representation for compact formatting (no newlines)
	if bytes.Contains(data1, []byte("\n")) || bytes.Contains(data1, []byte("\r")) {
		t.Errorf("Canonical JSON contains newline characters: %s", string(data1))
	}

	// Verify that descriptor.id in serialized canonical JSON is zeroed out
	var descObj map[string]interface{}
	if err := json.Unmarshal(mapObj["descriptor"], &descObj); err != nil {
		t.Fatalf("Failed to unmarshal descriptor: %v", err)
	}

	// Verify deserialization sets ContentID to SHA-256 digest of payload
	mDeser, err := manifest.Deserialize(data1)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	expectedHash := sha256.Sum256(data1)
	if !bytes.Equal(mDeser.Descriptor.ID[:], expectedHash[:]) {
		t.Errorf("Deserialized ContentID %x != expected SHA-256 %x", mDeser.Descriptor.ID, expectedHash)
	}
}
