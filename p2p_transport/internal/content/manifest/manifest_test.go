package manifest

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func createSampleManifest() *Manifest {
	var cid core.ContentID
	copy(cid[:], []byte("01234567890123456789012345678901"))

	var chunkID1, chunkID2 core.ChunkID
	copy(chunkID1[:], []byte("chunk111111111111111111111111111"))
	copy(chunkID2[:], []byte("chunk222222222222222222222222222"))

	var hash core.Hash
	copy(hash[:], []byte("hash1234567890123456789012345678"))

	return &Manifest{
		Version: 1,
		Descriptor: ContentDescriptor{
			ID:   cid,
			Type: TypeFile,
			Size: 1024,
		},
		ChunkIDs:   []core.ChunkID{chunkID1, chunkID2},
		MerkleRoot: hash,
		WholeHash:  hash,
		Crypto: CryptoDescriptor{
			Algorithm:      "chacha20-poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "key-01",
		},
	}
}

func TestManifest_SerializeDeserialize(t *testing.T) {
	original := createSampleManifest()
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.Version != original.Version {
		t.Errorf("Version mismatch: got %d, want %d", deserialized.Version, original.Version)
	}
	if deserialized.Descriptor.ID != original.Descriptor.ID {
		t.Errorf("Descriptor ID mismatch")
	}
	if len(deserialized.ChunkIDs) != len(original.ChunkIDs) {
		t.Errorf("ChunkIDs count mismatch")
	}
}

func TestManifest_DeserializeReader(t *testing.T) {
	original := createSampleManifest()
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	reader := bytes.NewReader(data)
	deserialized, err := DeserializeReader(reader)
	if err != nil {
		t.Fatalf("DeserializeReader failed: %v", err)
	}

	if deserialized.Descriptor.Size != original.Descriptor.Size {
		t.Errorf("Size mismatch: got %d, want %d", deserialized.Descriptor.Size, original.Descriptor.Size)
	}
}

func TestManifest_Deserialize_EmptyData(t *testing.T) {
	_, err := Deserialize([]byte{})
	if err == nil {
		t.Fatal("Expected error for empty manifest data, got nil")
	}
}

func TestManifest_DeserializeReader_NilReader(t *testing.T) {
	_, err := DeserializeReader(nil)
	if err == nil {
		t.Fatal("Expected error for nil reader, got nil")
	}
}

func TestManifest_Deserialize_ExceedsMaxManifestSize(t *testing.T) {
	oversized := make([]byte, MaxManifestSize+1)
	for i := range oversized {
		oversized[i] = '{'
	}

	_, err := Deserialize(oversized)
	if err == nil {
		t.Fatal("Expected error when deserializing oversized manifest byte slice, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds MaxManifestSize limit") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestManifest_DeserializeReader_ExceedsLimit(t *testing.T) {
	// Create a JSON object with padding that exceeds MaxManifestSize
	padding := strings.Repeat(" ", MaxManifestSize+100)
	rawJSON := fmt.Sprintf(`{"version":1, "padding":"%s"}`, padding)

	reader := strings.NewReader(rawJSON)
	_, err := DeserializeReader(reader)
	if err == nil {
		t.Fatal("Expected error when reading stream exceeding MaxManifestSize limit, got nil")
	}
}

func TestManifest_Deserialize_UnknownFields(t *testing.T) {
	jsonWithUnknown := `{"version":1, "unknown_field": "disallowed"}`
	_, err := Deserialize([]byte(jsonWithUnknown))
	if err == nil {
		t.Fatal("Expected error for JSON with unknown fields, got nil")
	}
}

func TestManifest_Deserialize_Concurrency(t *testing.T) {
	original := createSampleManifest()
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	var wg sync.WaitGroup
	concurrency := 50
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, err := Deserialize(data)
			if err != nil {
				t.Errorf("Concurrent Deserialize failed: %v", err)
				return
			}
			if m.Version != original.Version {
				t.Errorf("Concurrent Deserialize version mismatch")
			}
		}()
	}
	wg.Wait()
}
