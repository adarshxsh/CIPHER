package manifest_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifest_SerializeDeserialize_Valid(t *testing.T) {
	var id core.ContentID
	id[0] = 0x12

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   id,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01},
		},
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "XChaCha20-Poly1305",
			Version:   1,
			KeyID:     "key-1",
		},
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	if int64(len(data)) > manifest.MaxManifestJSONSize {
		t.Fatalf("Serialized data %d bytes exceeds MaxManifestJSONSize", len(data))
	}

	m2, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if m2.Descriptor.ID != m.Descriptor.ID {
		t.Fatalf("ContentID mismatch: expected %x, got %x", m.Descriptor.ID, m2.Descriptor.ID)
	}
	if m2.Descriptor.Size != m.Descriptor.Size {
		t.Fatalf("Size mismatch: expected %d, got %d", m.Descriptor.Size, m2.Descriptor.Size)
	}
}

func TestManifest_Deserialize_OversizedPayload(t *testing.T) {
	// Create payload > MaxManifestJSONSize
	oversized := make([]byte, manifest.MaxManifestJSONSize+1)
	for i := range oversized {
		oversized[i] = ' '
	}

	_, err := manifest.Deserialize(oversized)
	if err == nil {
		t.Fatal("expected error for oversized manifest payload, got nil")
	}
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}

func TestManifest_Deserialize_EmptyPayload(t *testing.T) {
	_, err := manifest.Deserialize(nil)
	if err == nil {
		t.Fatal("expected error for empty manifest payload, got nil")
	}

	_, err = manifest.Deserialize([]byte{})
	if err == nil {
		t.Fatal("expected error for empty byte slice, got nil")
	}
}

func TestManifest_Validate_StringFieldLimits(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.ContentType(strings.Repeat("a", manifest.MaxStringLength+1)),
		},
	}

	err := m.Validate()
	if err == nil {
		t.Fatal("expected error for oversized Descriptor.Type, got nil")
	}
	if !errors.Is(err, manifest.ErrStringFieldTooLong) {
		t.Fatalf("expected ErrStringFieldTooLong, got %v", err)
	}

	m = &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
		},
		Crypto: manifest.CryptoDescriptor{
			Algorithm: strings.Repeat("b", manifest.MaxStringLength+1),
		},
	}

	err = m.Validate()
	if err == nil {
		t.Fatal("expected error for oversized Crypto.Algorithm, got nil")
	}
	if !errors.Is(err, manifest.ErrStringFieldTooLong) {
		t.Fatalf("expected ErrStringFieldTooLong, got %v", err)
	}
}

func TestManifest_Deserialize_InvalidJSON(t *testing.T) {
	invalidJSON := []byte("{invalid_json}")
	_, err := manifest.Deserialize(invalidJSON)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode manifest JSON") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestManifest_Deserialize_TruncatedAtLimit(t *testing.T) {
	// Create JSON that is valid if full, but truncated when read up to limit
	padding := strings.Repeat(" ", int(manifest.MaxManifestJSONSize)-20)
	raw := `{"version":1,` + padding + `"descriptor":{"size":100}}`
	buf := bytes.NewBufferString(raw)
	// Truncate to just over limit so Deserialize fails limit check
	data := buf.Bytes()
	if int64(len(data)) <= manifest.MaxManifestJSONSize {
		// Append extra bytes to push over MaxManifestJSONSize
		extra := make([]byte, int(manifest.MaxManifestJSONSize)-len(data)+10)
		data = append(data, extra...)
	}

	_, err := manifest.Deserialize(data)
	if err == nil {
		t.Fatal("expected error for payload exceeding limit, got nil")
	}
}
