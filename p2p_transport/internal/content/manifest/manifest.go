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
	// MaxManifestSize limits the payload size for manifests (2MB frame minus 3-byte envelope).
	MaxManifestSize = 2*1024*1024 - 3
)

var (
	// MaxSupportedChunkCount limits the number of chunk IDs in a manifest.
	MaxSupportedChunkCount uint64 = 1 << 32

	ErrManifestTooLarge = errors.New("manifest exceeds maximum payload size")
	ErrTooManyChunks    = errors.New("manifest chunk count exceeds maximum supported count")
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

// Deserialize parses raw manifest bytes into a Manifest struct, enforcing upper payload boundaries and chunk count limits.
func Deserialize(data []byte) (*Manifest, error) {
	if len(data) > MaxManifestSize {
		return nil, fmt.Errorf("%w: received %d bytes, max %d bytes", ErrManifestTooLarge, len(data), MaxManifestSize)
	}

	limitedReader := io.LimitReader(bytes.NewReader(data), int64(MaxManifestSize))
	var m Manifest
	if err := json.NewDecoder(limitedReader).Decode(&m); err != nil {
		return nil, err
	}

	if uint64(len(m.ChunkIDs)) > MaxSupportedChunkCount {
		return nil, fmt.Errorf("%w: received %d chunk IDs, max %d", ErrTooManyChunks, len(m.ChunkIDs), MaxSupportedChunkCount)
	}

	return &m, nil
}

// DeserializeFromReader parses a Manifest from an io.Reader stream using io.LimitReader to enforce stream ceilings.
func DeserializeFromReader(r io.Reader) (*Manifest, error) {
	if r == nil {
		return nil, errors.New("reader is nil")
	}

	limited := io.LimitReader(r, int64(MaxManifestSize)+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest stream: %w", err)
	}

	return Deserialize(data)
}
