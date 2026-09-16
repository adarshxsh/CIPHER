package manifest_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestCalculateContentID_DeterministicKeyOrdering(t *testing.T) {
	// JSON with keys in arbitrary order and extra spaces
	json1 := []byte(`{
		"version": 1,
		"crypto": {
			"version": 1,
			"key_id": "test-key",
			"chunk_nonce_size": 12,
			"algorithm": "ChaCha20-Poly1305"
		},
		"whole_hash": "0000000000000000000000000000000000000000000000000000000000000000",
		"merkle_root": "0000000000000000000000000000000000000000000000000000000000000000",
		"descriptor": {
			"size": 1024,
			"type": "file"
		},
		"chunk_ids": []
	}`)

	// Same semantic content, different key order and formatting
	json2 := []byte(`{"chunk_ids":[],"descriptor":{"type":"file","size":1024},"merkle_root":"0000000000000000000000000000000000000000000000000000000000000000","whole_hash":"0000000000000000000000000000000000000000000000000000000000000000","crypto":{"algorithm":"ChaCha20-Poly1305","chunk_nonce_size":12,"key_id":"test-key","version":1},"version":1}`)

	id1, canonical1, err1 := manifest.CalculateContentID(json1)
	if err1 != nil {
		t.Fatalf("CalculateContentID failed for json1: %v", err1)
	}

	id2, canonical2, err2 := manifest.CalculateContentID(json2)
	if err2 != nil {
		t.Fatalf("CalculateContentID failed for json2: %v", err2)
	}

	if id1 != id2 {
		t.Errorf("ContentIDs do not match: %x vs %x", id1, id2)
	}

	if !bytes.Equal(canonical1, canonical2) {
		t.Errorf("Canonical bytes do not match:\n%s\nvs\n%s", canonical1, canonical2)
	}
}

func TestCalculateContentID_IgnoresDescriptorID(t *testing.T) {
	jsonWithoutID := []byte(`{
		"version": 1,
		"descriptor": {"type": "file", "size": 2048},
		"chunk_ids": [],
		"merkle_root": "0000000000000000000000000000000000000000000000000000000000000000",
		"whole_hash": "0000000000000000000000000000000000000000000000000000000000000000",
		"crypto": {"algorithm": "ChaCha20-Poly1305", "version": 1, "chunk_nonce_size": 12, "key_id": "k1"}
	}`)

	jsonWithID := []byte(`{
		"version": 1,
		"descriptor": {"id": "11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff", "type": "file", "size": 2048},
		"chunk_ids": [],
		"merkle_root": "0000000000000000000000000000000000000000000000000000000000000000",
		"whole_hash": "0000000000000000000000000000000000000000000000000000000000000000",
		"crypto": {"algorithm": "ChaCha20-Poly1305", "version": 1, "chunk_nonce_size": 12, "key_id": "k1"}
	}`)

	id1, _, err1 := manifest.CalculateContentID(jsonWithoutID)
	if err1 != nil {
		t.Fatalf("CalculateContentID failed: %v", err1)
	}

	id2, _, err2 := manifest.CalculateContentID(jsonWithID)
	if err2 != nil {
		t.Fatalf("CalculateContentID failed: %v", err2)
	}

	if id1 != id2 {
		t.Fatalf("ContentIDs differ when descriptor.id is present vs absent: %x vs %x", id1, id2)
	}
}

func TestDeserializeAndVerify_LegitimateAndAltered(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 5000,
		},
		ChunkIDs:  []core.ChunkID{{1, 2, 3}},
		WholeHash: core.Hash{9, 9, 9},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	expectedID, _, err := m.CalculateContentID()
	if err != nil {
		t.Fatalf("CalculateContentID failed: %v", err)
	}
	m.Descriptor.ID = expectedID

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// 1. Legitimate manifest passes resolution and digest checks
	resM, err := manifest.DeserializeAndVerify(data, expectedID)
	if err != nil {
		t.Fatalf("DeserializeAndVerify failed for legitimate manifest: %v", err)
	}
	if resM.Descriptor.ID != expectedID {
		t.Errorf("Expected ContentID %x, got %x", expectedID, resM.Descriptor.ID)
	}

	// 2. Altered manifest payload fails digest verification and is rejected
	var rawMap map[string]any
	if err := json.Unmarshal(data, &rawMap); err != nil {
		t.Fatalf("Failed to unmarshal to map: %v", err)
	}

	// Tamper with size field
	rawMap["descriptor"].(map[string]any)["size"] = 99999
	alteredData, err := json.Marshal(rawMap)
	if err != nil {
		t.Fatalf("Failed to marshal altered map: %v", err)
	}

	_, err = manifest.DeserializeAndVerify(alteredData, expectedID)
	if err == nil {
		t.Fatalf("Expected error when deserializing altered manifest payload, but got nil")
	}
}
