package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"cipher/internal/content/core"
)

const (
	// MaxManifestSize specifies the maximum allowed byte length for a manifest JSON payload (2MB).
	MaxManifestSize = 2 * 1024 * 1024
)

var (
	ErrManifestTooLarge = errors.New("manifest payload exceeds maximum size")
)

type ContentType string

const (
	TypeFile ContentType = "file"
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

func Deserialize(data []byte) (*Manifest, error) {
	if len(data) > MaxManifestSize {
		return nil, ErrManifestTooLarge
	}
	return DeserializeReader(bytes.NewReader(data))
}

func DeserializeReader(r io.Reader) (*Manifest, error) {
	if r == nil {
		return nil, errors.New("reader is nil")
	}
	limited := io.LimitReader(r, MaxManifestSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxManifestSize {
		return nil, ErrManifestTooLarge
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
