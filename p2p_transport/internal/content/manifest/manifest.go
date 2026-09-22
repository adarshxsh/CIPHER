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

	// MaxManifestSize limits manifest JSON byte size before deserialization (512 KiB).
	MaxManifestSize = 512 * 1024

	// MaxSupportedChunkCount caps the maximum allowed slice length for chunk IDs.
	MaxSupportedChunkCount uint64 = 1 << 32
)

var (
	ErrEmptyManifest     = errors.New("manifest data is empty")
	ErrManifestTooLarge  = errors.New("manifest data exceeds maximum allowed size")
	ErrInvalidChunkCount = errors.New("manifest chunk count exceeds maximum allowed count")
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
		return nil, ErrEmptyManifest
	}
	if len(data) > MaxManifestSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrManifestTooLarge, len(data), MaxManifestSize)
	}

	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(data), MaxManifestSize))
	decoder.DisallowUnknownFields()

	var m Manifest
	if err := decoder.Decode(&m); err != nil {
		return nil, err
	}

	if uint64(len(m.ChunkIDs)) > MaxSupportedChunkCount {
		return nil, fmt.Errorf("%w: %d > %d", ErrInvalidChunkCount, len(m.ChunkIDs), MaxSupportedChunkCount)
	}

	return &m, nil
}
