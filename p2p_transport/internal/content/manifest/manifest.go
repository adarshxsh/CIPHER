package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cipher/internal/content/core"
)

const (
	// MaxManifestJSONSize is the maximum allowed byte length for manifest JSON payloads (256 KiB).
	MaxManifestJSONSize int64 = 256 * 1024

	// MaxSupportedChunkCount is the maximum allowed number of chunk IDs in a manifest.
	MaxSupportedChunkCount uint64 = 1 << 32

	// MaxStringLength is the maximum allowed character/byte length for manifest string fields.
	MaxStringLength int = 4096
)

var (
	ErrManifestTooLarge   = errors.New("manifest payload exceeds maximum size limit")
	ErrTooManyChunks      = errors.New("manifest chunk count exceeds maximum limit")
	ErrStringFieldTooLong = errors.New("manifest string field exceeds maximum length")
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

func (m *Manifest) Validate() error {
	if uint64(len(m.ChunkIDs)) > MaxSupportedChunkCount {
		return fmt.Errorf("%w: received %d, maximum supported %d", ErrTooManyChunks, len(m.ChunkIDs), MaxSupportedChunkCount)
	}
	if len(m.Descriptor.Type) > MaxStringLength {
		return fmt.Errorf("%w: descriptor type length %d exceeds maximum %d", ErrStringFieldTooLong, len(m.Descriptor.Type), MaxStringLength)
	}
	if len(m.Crypto.Algorithm) > MaxStringLength {
		return fmt.Errorf("%w: crypto algorithm length %d exceeds maximum %d", ErrStringFieldTooLong, len(m.Crypto.Algorithm), MaxStringLength)
	}
	if len(m.Crypto.KeyID) > MaxStringLength {
		return fmt.Errorf("%w: crypto key_id length %d exceeds maximum %d", ErrStringFieldTooLong, len(m.Crypto.KeyID), MaxStringLength)
	}
	return nil
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
		return nil, errors.New("empty manifest payload")
	}
	if int64(len(data)) > MaxManifestJSONSize {
		return nil, fmt.Errorf("%w: received %d bytes, maximum %d bytes", ErrManifestTooLarge, len(data), MaxManifestJSONSize)
	}

	reader := io.LimitReader(bytes.NewReader(data), MaxManifestJSONSize)
	decoder := json.NewDecoder(reader)

	var m Manifest
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode manifest JSON: %w", err)
	}

	if err := m.Validate(); err != nil {
		return nil, err
	}

	return &m, nil
}
