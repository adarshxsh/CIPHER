package manifest

import (
	"encoding/json"
	"errors"
	"fmt"

	"cipher/internal/content/core"
)

var (
	ErrInvalidChunkCount = errors.New("invalid manifest chunk count")
)

const (
	// StandardChunkSize is the default plaintext chunk size (32 KiB).
	StandardChunkSize uint64 = 32 * 1024

	// MaxSupportedChunkCount prevents excessive chunk allocations (1 << 32).
	MaxSupportedChunkCount uint64 = 1 << 32
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
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	chunkCount := uint64(len(m.ChunkIDs))
	if chunkCount > MaxSupportedChunkCount {
		return nil, fmt.Errorf("%w: chunk count %d exceeds maximum supported limit %d", ErrInvalidChunkCount, chunkCount, MaxSupportedChunkCount)
	}

	var expectedChunks uint64
	if m.Descriptor.Size > 0 {
		expectedChunks = m.Descriptor.Size / StandardChunkSize
		if m.Descriptor.Size%StandardChunkSize != 0 {
			expectedChunks++
		}
	}

	if chunkCount > expectedChunks {
		return nil, fmt.Errorf("%w: chunk count %d exceeds calculated chunk count %d for content size %d", ErrInvalidChunkCount, chunkCount, expectedChunks, m.Descriptor.Size)
	}

	return &m, nil
}
