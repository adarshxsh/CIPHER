package manifest_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func createValidManifest() *manifest.Manifest {
	var cid core.ContentID
	cid[0] = 0x01
	cid[31] = 0x02

	var hash core.Hash
	hash[0] = 0xAA

	var chunkID1, chunkID2 core.ChunkID
	chunkID1[0] = 0x10
	chunkID2[0] = 0x20

	return &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 65536,
		},
		ChunkIDs:   []core.ChunkID{chunkID1, chunkID2},
		MerkleRoot: hash,
		WholeHash:  hash,
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "test-key",
		},
	}
}

func TestManifest_ValidSerializeDeserialize(t *testing.T) {
	m := createValidManifest()

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Version != m.Version {
		t.Errorf("Version mismatch: got %d, want %d", deserialized.Version, m.Version)
	}
	if deserialized.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("ContentID mismatch")
	}
	if len(deserialized.ChunkIDs) != len(m.ChunkIDs) {
		t.Errorf("ChunkIDs length mismatch: got %d, want %d", len(deserialized.ChunkIDs), len(m.ChunkIDs))
	}
}

func TestManifest_DeserializeReader(t *testing.T) {
	m := createValidManifest()
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := manifest.DeserializeReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeserializeReader failed: %v", err)
	}

	if deserialized.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("ContentID mismatch")
	}
}

func TestManifest_RejectUnknownFields(t *testing.T) {
	m := createValidManifest()
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Inject unknown JSON field
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal to map failed: %v", err)
	}
	raw["unexpected_malicious_field"] = "payload_inflation"

	maliciousData, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("Marshal malicious data failed: %v", err)
	}

	_, err = manifest.Deserialize(maliciousData)
	if err == nil {
		t.Fatalf("Expected error for manifest with unknown fields, got nil")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("Expected 'unknown field' error, got: %v", err)
	}
}

func TestManifest_RejectOversizedManifest(t *testing.T) {
	// Create payload exceeding MaxManifestSize (512 KiB)
	oversizedLen := manifest.MaxManifestSize + 100
	hugePadding := strings.Repeat("a", int(oversizedLen))

	hugeData := []byte(fmt.Sprintf(`{"version":1,"padding":"%s"}`, hugePadding))

	_, err := manifest.Deserialize(hugeData)
	if err == nil {
		t.Fatalf("Expected error for oversized manifest byte slice, got nil")
	}
	if !errors.Is(err, manifest.ErrOversizedManifest) && !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("Expected ErrOversizedManifest error, got: %v", err)
	}

	// Test streaming reader with oversized payload
	_, err = manifest.DeserializeReader(bytes.NewReader(hugeData))
	if err == nil {
		t.Fatalf("Expected error for oversized manifest reader, got nil")
	}
	if !errors.Is(err, manifest.ErrOversizedManifest) && !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("Expected ErrOversizedManifest error, got: %v", err)
	}
}

func TestManifest_RejectNilReader(t *testing.T) {
	_, err := manifest.DeserializeReader(nil)
	if err == nil {
		t.Fatalf("Expected error for nil reader, got nil")
	}
	if !errors.Is(err, manifest.ErrNilReader) {
		t.Errorf("Expected ErrNilReader, got: %v", err)
	}
}

func TestManifest_ValidateChunkCount(t *testing.T) {
	m := createValidManifest()
	if err := m.Validate(); err != nil {
		t.Fatalf("Valid manifest validation failed: %v", err)
	}

	nilManifest := (*manifest.Manifest)(nil)
	if err := nilManifest.Validate(); err == nil {
		t.Errorf("Expected error for nil manifest validation")
	}
}
