package manifest_test

import (
	"crypto/sha256"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifest_SerializeAndDeserialize(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 2, 3},
		},
		MerkleRoot: core.Hash{4, 5, 6},
		WholeHash:  core.Hash{4, 5, 6},
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

	expectedID := sha256.Sum256(data)

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Descriptor.ID != expectedID {
		t.Fatalf("expected ContentID %x, got %x", expectedID, deserialized.Descriptor.ID)
	}

	deserializedAndVerified, err := manifest.DeserializeAndVerify(data, expectedID)
	if err != nil {
		t.Fatalf("DeserializeAndVerify failed: %v", err)
	}
	if deserializedAndVerified.Descriptor.ID != expectedID {
		t.Fatalf("expected ContentID %x, got %x", expectedID, deserializedAndVerified.Descriptor.ID)
	}
}

func TestManifest_DeserializeAndVerify_RejectsSpoofed(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 2, 3},
		},
		MerkleRoot: core.Hash{4, 5, 6},
		WholeHash:  core.Hash{4, 5, 6},
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

	expectedID := sha256.Sum256(data)

	// Tamper with data
	m.Descriptor.Size = 2048
	tamperedData, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	_, err = manifest.DeserializeAndVerify(tamperedData, expectedID)
	if err == nil {
		t.Fatalf("expected DeserializeAndVerify to fail for tampered manifest payload")
	}
}
