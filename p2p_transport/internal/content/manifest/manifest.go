package manifest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"cipher/internal/content/core"
)

var ErrIntegrityMismatch = errors.New("integrity mismatch")

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

// ComputeID returns the canonical ContentID for the manifest by computing the SHA-256 digest
// of its JSON representation with Descriptor.ID zeroed out.
func (m *Manifest) ComputeID() (core.ContentID, error) {
	if m == nil {
		return core.ContentID{}, errors.New("nil manifest")
	}
	mCopy := *m
	mCopy.Descriptor.ID = core.ContentID{}
	data, err := mCopy.Serialize()
	if err != nil {
		return core.ContentID{}, err
	}
	return sha256.Sum256(data), nil
}

// ValidateManifestID deserializes the manifest JSON payload, zeros Descriptor.ID,
// computes its canonical SHA-256 digest, and verifies that it matches expectedID.
func ValidateManifestID(data []byte, expectedID core.ContentID) error {
	m, err := Deserialize(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize manifest: %w", err)
	}

	if m.Descriptor.ID != expectedID {
		return fmt.Errorf("%w: descriptor content ID %x does not match expected ID %x", ErrIntegrityMismatch, m.Descriptor.ID, expectedID)
	}

	computedID, err := m.ComputeID()
	if err != nil {
		return fmt.Errorf("failed to compute manifest ID: %w", err)
	}

	if computedID != expectedID {
		return fmt.Errorf("%w: computed digest %x does not match expected ID %x", ErrIntegrityMismatch, computedID, expectedID)
	}

	return nil
}

