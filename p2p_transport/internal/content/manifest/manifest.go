package manifest

import (
	"encoding/json"
	"errors"
	"io"

	"cipher/internal/content/core"
)

const (
	// MaxManifestSize defines the maximum allowed byte size for a manifest payload or stream.
	MaxManifestSize = 2 * 1024 * 1024 // 2 MiB
)

var (
	ErrManifestTooLarge = errors.New("manifest payload exceeds maximum size limit")
	ErrNilReader        = errors.New("reader is nil")
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
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DeserializeFromStream reads manifest data from an io.Reader up to MaxManifestSize + 1 bytes
// and deserializes it into a Manifest struct. If the stream contains more than MaxManifestSize
// bytes, it returns ErrManifestTooLarge without reading further or exhausting memory.
func DeserializeFromStream(r io.Reader) (*Manifest, error) {
	if r == nil {
		return nil, ErrNilReader
	}
	limitedR := io.LimitReader(r, int64(MaxManifestSize)+1)
	data, err := io.ReadAll(limitedR)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxManifestSize {
		return nil, ErrManifestTooLarge
	}
	return Deserialize(data)
}

// DeserializeStream is an alias for DeserializeFromStream.
func DeserializeStream(r io.Reader) (*Manifest, error) {
	return DeserializeFromStream(r)
}

// DeserializeReader is an alias for DeserializeFromStream.
func DeserializeReader(r io.Reader) (*Manifest, error) {
	return DeserializeFromStream(r)
}
