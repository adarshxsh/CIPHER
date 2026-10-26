package manifest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"

	"cipher/internal/content/core"
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
	return &m, nil
}

// CanonicalBytes serializes the manifest JSON with Descriptor.ID set to 32 zero bytes.
func (m *Manifest) CanonicalBytes() ([]byte, error) {
	if m == nil {
		return nil, errors.New("manifest is nil")
	}
	originalID := m.Descriptor.ID
	m.Descriptor.ID = core.ContentID{}
	defer func() {
		m.Descriptor.ID = originalID
	}()
	return json.Marshal(m)
}

// ComputeDigest calculates a SHA-256 hash digest over the canonical manifest JSON
// payload with Descriptor.ID set to 32 zero bytes.
func (m *Manifest) ComputeDigest() (core.ContentID, error) {
	data, err := m.CanonicalBytes()
	if err != nil {
		return core.ContentID{}, err
	}
	hash := sha256.Sum256(data)
	return core.ContentID(hash), nil
}

// ComputeContentID is an alias for ComputeDigest.
func (m *Manifest) ComputeContentID() (core.ContentID, error) {
	return m.ComputeDigest()
}

