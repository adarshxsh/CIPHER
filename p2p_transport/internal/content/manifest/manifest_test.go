package manifest_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_ValidManifest(t *testing.T) {
	var contentID core.ContentID
	contentID[0] = 0x01

	var chunkID core.ChunkID
	chunkID[0] = 0x02

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{chunkID},
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "XChaCha20-Poly1305",
			Version:   1,
			KeyID:     "key-123",
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

	if deserialized.Version != m.Version {
		t.Errorf("expected version %d, got %d", m.Version, deserialized.Version)
	}
	if len(deserialized.ChunkIDs) != 1 {
		t.Errorf("expected 1 chunk ID, got %d", len(deserialized.ChunkIDs))
	}
	if deserialized.Crypto.Algorithm != "XChaCha20-Poly1305" {
		t.Errorf("expected algorithm XChaCha20-Poly1305, got %s", deserialized.Crypto.Algorithm)
	}
}

func TestDeserialize_RejectsOversizedPayload(t *testing.T) {
	oversized := make([]byte, manifest.MaxManifestSize+1)
	// Make it look like JSON start to avoid immediate JSON parse syntax error if evaluated before size check
	copy(oversized, []byte(`{"version": 1, "extra": "`))
	for i := 25; i < len(oversized)-2; i++ {
		oversized[i] = 'A'
	}
	oversized[len(oversized)-2] = '"'
	oversized[len(oversized)-1] = '}'

	_, err := manifest.Deserialize(oversized)
	if err == nil {
		t.Fatal("expected error for oversized manifest payload, got nil")
	}
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_RejectsOversizedChunkIDsArray(t *testing.T) {
	chunkIDs := make([]core.ChunkID, manifest.MaxManifestChunkCount+1)
	m := &manifest.Manifest{
		Version:  1,
		ChunkIDs: chunkIDs,
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "test",
			KeyID:     "key",
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if err == nil {
		t.Fatal("expected error for excessive ChunkIDs count, got nil")
	}
	if !errors.Is(err, manifest.ErrInvalidManifestStructure) {
		t.Fatalf("expected ErrInvalidManifestStructure, got %v", err)
	}
}

func TestDeserialize_RejectsOversizedCryptoAlgorithm(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Crypto: manifest.CryptoDescriptor{
			Algorithm: strings.Repeat("A", manifest.MaxAlgorithmNameSize+1),
			KeyID:     "key",
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if err == nil {
		t.Fatal("expected error for oversized algorithm string, got nil")
	}
	if !errors.Is(err, manifest.ErrInvalidManifestStructure) {
		t.Fatalf("expected ErrInvalidManifestStructure, got %v", err)
	}
}

func TestDeserialize_RejectsOversizedCryptoKeyID(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "AES-GCM",
			KeyID:     strings.Repeat("K", manifest.MaxKeyIDSize+1),
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if err == nil {
		t.Fatal("expected error for oversized key_id string, got nil")
	}
	if !errors.Is(err, manifest.ErrInvalidManifestStructure) {
		t.Fatalf("expected ErrInvalidManifestStructure, got %v", err)
	}
}

func TestDeserialize_BoundedStreamDecoder(t *testing.T) {
	// Construct JSON payload that is truncated due to limit or malformed
	var buf bytes.Buffer
	buf.WriteString(`{"version": 1, "descriptor": {"size": 100}, "crypto": {"algorithm": "valid"}}`)
	buf.WriteString(strings.Repeat(" ", 10))

	m, err := manifest.Deserialize(buf.Bytes())
	if err != nil {
		t.Fatalf("expected valid decoding, got: %v", err)
	}
	if m.Descriptor.Size != 100 {
		t.Errorf("expected size 100, got %d", m.Descriptor.Size)
	}
}
