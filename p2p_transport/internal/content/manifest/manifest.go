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
	// MaxManifestSize limits incoming manifest bytes to match protocol frame payload cap (2MB - 3 bytes header).
	MaxManifestSize = 2*1024*1024 - 3

	// MaxSupportedChunkCount limits chunk array entries in manifest to prevent memory amplification.
	MaxSupportedChunkCount uint64 = 1 << 32
)

var (
	ErrManifestTooLarge = errors.New("manifest exceeds maximum allowed size")
	ErrTooManyChunks    = errors.New("manifest chunk count exceeds maximum supported limit")
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
		return nil, fmt.Errorf("%w: received=%d maximum=%d", ErrManifestTooLarge, len(data), MaxManifestSize)
	}

	limited := io.LimitReader(bytes.NewReader(data), int64(MaxManifestSize))
	var m Manifest
	dec := json.NewDecoder(limited)
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}

	if uint64(len(m.ChunkIDs)) > MaxSupportedChunkCount {
		return nil, fmt.Errorf("%w: count=%d maximum=%d", ErrTooManyChunks, len(m.ChunkIDs), MaxSupportedChunkCount)
	}

	return &m, nil
}

func DeserializeReader(r io.Reader) (*Manifest, error) {
	if r == nil {
		return nil, errors.New("nil reader")
	}

	lr := io.LimitReader(r, int64(MaxManifestSize)+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}

	if len(data) > MaxManifestSize {
		return nil, fmt.Errorf("%w: received=%d maximum=%d", ErrManifestTooLarge, len(data), MaxManifestSize)
	}

	return Deserialize(data)
}
