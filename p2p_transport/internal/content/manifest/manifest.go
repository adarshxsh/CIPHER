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
	// MaxManifestSize specifies the maximum allowed serialized manifest size in bytes (256 KiB).
	MaxManifestSize int64 = 256 * 1024

	// MaxManifestChunkCount limits the maximum array size of ChunkIDs in a single manifest.
	MaxManifestChunkCount = 2048

	// MaxSupportedChunkCount limits chunk count protocol-wide.
	MaxSupportedChunkCount uint64 = 1 << 32

	// MaxAlgorithmNameSize limits string length for Crypto.Algorithm.
	MaxAlgorithmNameSize = 64

	// MaxKeyIDSize limits string length for Crypto.KeyID.
	MaxKeyIDSize = 256
)

var (
	ErrManifestTooLarge         = errors.New("manifest data exceeds size limit")
	ErrInvalidManifestStructure = errors.New("manifest structural validation failed")
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
	if int64(len(data)) > MaxManifestSize {
		return nil, fmt.Errorf("%w: size %d exceeds limit of %d bytes", ErrManifestTooLarge, len(data), MaxManifestSize)
	}

	limitReader := io.LimitReader(bytes.NewReader(data), MaxManifestSize)
	dec := json.NewDecoder(limitReader)

	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}

	if err := m.ValidateBounds(); err != nil {
		return nil, err
	}

	return &m, nil
}

func (m *Manifest) ValidateBounds() error {
	if uint64(len(m.ChunkIDs)) > MaxSupportedChunkCount || len(m.ChunkIDs) > MaxManifestChunkCount {
		return fmt.Errorf(
			"%w: chunk count %d exceeds maximum limit",
			ErrInvalidManifestStructure,
			len(m.ChunkIDs),
		)
	}

	if len(m.Crypto.Algorithm) > MaxAlgorithmNameSize {
		return fmt.Errorf(
			"%w: crypto algorithm string length %d exceeds max %d",
			ErrInvalidManifestStructure,
			len(m.Crypto.Algorithm),
			MaxAlgorithmNameSize,
		)
	}

	if len(m.Crypto.KeyID) > MaxKeyIDSize {
		return fmt.Errorf(
			"%w: crypto key_id string length %d exceeds max %d",
			ErrInvalidManifestStructure,
			len(m.Crypto.KeyID),
			MaxKeyIDSize,
		)
	}

	return nil
}
