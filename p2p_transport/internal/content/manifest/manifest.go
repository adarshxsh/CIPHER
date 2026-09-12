package manifest

import (
	"encoding/json"
	"errors"
	"fmt"

	"cipher/internal/content/core"
)

const (
	// MaxManifestJSONSize is the maximum size (in bytes) allowed for manifest JSON data
	// prior to decoding, derived from MaxManifestSize (2,097,149) - ContentIDSize (32).
	MaxManifestJSONSize = 2097117
)

var (
	ErrManifestEmpty    = errors.New("manifest data is empty")
	ErrManifestTooLarge = errors.New("manifest data exceeds maximum allowed size limit")
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
	if len(data) == 0 {
		return nil, ErrManifestEmpty
	}
	if len(data) > MaxManifestJSONSize {
		return nil, fmt.Errorf("%w: received %d bytes, maximum allowed is %d", ErrManifestTooLarge, len(data), MaxManifestJSONSize)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
