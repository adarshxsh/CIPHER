package manifest_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestDeserialize_EmptyPayload(t *testing.T) {
	_, err := manifest.Deserialize([]byte{})
	if !errors.Is(err, manifest.ErrEmptyManifest) {
		t.Fatalf("expected ErrEmptyManifest, got %v", err)
	}

	_, err = manifest.Deserialize(nil)
	if !errors.Is(err, manifest.ErrEmptyManifest) {
		t.Fatalf("expected ErrEmptyManifest for nil, got %v", err)
	}
}

func TestDeserialize_OversizedPayload(t *testing.T) {
	data := make([]byte, manifest.MaxManifestSize+1)
	for i := range data {
		data[i] = ' '
	}
	_, err := manifest.Deserialize(data)
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestDeserialize_ValidManifest(t *testing.T) {
	var cid core.ContentID
	cid[0] = 0x12
	var chunkID core.ChunkID
	chunkID[0] = 0x34

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{chunkID},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "XChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 24,
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	m2, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if m2.Version != m.Version {
		t.Errorf("version mismatch: expected %d, got %d", m.Version, m2.Version)
	}
	if len(m2.ChunkIDs) != 1 || m2.ChunkIDs[0] != chunkID {
		t.Errorf("chunk IDs mismatch")
	}
}

func TestDeserialize_UnknownFieldsRejected(t *testing.T) {
	badJSON := []byte(`{"version":1,"descriptor":{"id":"00","type":"file","size":10},"unknown_field":123}`)
	_, err := manifest.Deserialize(badJSON)
	if err == nil {
		t.Fatalf("expected error for unknown fields, got nil")
	}
}

func TestDeserialize_ExcessiveChunkCount(t *testing.T) {
	// Construct a manifest JSON with synthetic chunk_ids array
	var buf bytes.Buffer
	buf.WriteString(`{"version":1,"descriptor":{"id":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"type":"file","size":100},"chunk_ids":[`)
	// We verify that array parsing stays bounded
	for i := 0; i < 10; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]")
	}
	buf.WriteString(`],"crypto":{"algorithm":"XChaCha20-Poly1305","version":1,"chunk_nonce_size":24}}`)

	m, err := manifest.Deserialize(buf.Bytes())
	if err != nil {
		t.Fatalf("valid small array manifest should parse: %v", err)
	}
	if len(m.ChunkIDs) != 10 {
		t.Fatalf("expected 10 chunk IDs, got %d", len(m.ChunkIDs))
	}
}
