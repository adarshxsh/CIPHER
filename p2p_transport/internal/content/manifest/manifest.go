package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cipher/internal/content/core"
)

type ContentType string

const (
	TypeFile ContentType = "file"
)

const (
	// MaxManifestSize specifies the maximum allowed size for manifest JSON unmarshaling (2 MiB).
	MaxManifestSize = 2 * 1024 * 1024
	// MaxManifestJSONSize specifies the secondary limit boundary for manifest JSON payloads (256 KiB).
	MaxManifestJSONSize = 256 * 1024
)

type ContentDescriptor struct {
	ID   core.ContentID `json:"id"`
	Type ContentType    `json:"type"`
	Size uint64         `json:"size"`
}

type CryptoDescriptor struct {
	Algorithm      string `json:"algorithm"`
	Version        uint16 `json:"version"`
	ChunkNonceSize uint16 `json:"chunk_nonce_size"`
	KeyID          string `json:"key_id"` // Reference to the key
}

// Manifest represents the capability to understand the immutable content.
type Manifest struct {
	Version    uint16            `json:"version"`
	Descriptor ContentDescriptor `json:"descriptor"`
	ChunkIDs   []core.ChunkID    `json:"chunk_ids"`
	MerkleRoot core.Hash         `json:"merkle_root"` // Set to WholeHash for Milestone 7
	WholeHash  core.Hash         `json:"whole_hash"`
	Crypto     CryptoDescriptor  `json:"crypto"`
}

// UserMetadata represents mutable metadata not essential to the content's integrity.
type UserMetadata struct {
	Filename  string `json:"filename"`
	MimeType  string `json:"mime_type"`
	CreatedAt int64  `json:"created_at"`
}

func (m *Manifest) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

// DeserializeReader deserializes a Manifest from an io.Reader using a LimitedReader wrapper
// bounded by MaxManifestSize before JSON deserialization to prevent memory exhaustion.
func DeserializeReader(r io.Reader) (*Manifest, error) {
	if r == nil {
		return nil, errors.New("nil reader provided for manifest deserialization")
	}
	limited := io.LimitReader(r, int64(MaxManifestSize))
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()

	var m Manifest
	if err := decoder.Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Deserialize deserializes a Manifest from a byte slice, enforcing MaxManifestSize boundaries
// using a limited JSON reader wrapper before json.Unmarshal deserialization.
func Deserialize(data []byte) (*Manifest, error) {
	if len(data) == 0 {
		return nil, errors.New("empty manifest data")
	}
	if len(data) > MaxManifestSize {
		return nil, fmt.Errorf("manifest size %d exceeds MaxManifestSize limit of %d bytes", len(data), MaxManifestSize)
	}
	return DeserializeReader(bytes.NewReader(data))
}
