package manifest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"cipher/internal/content/core"
)

var ErrInvalidManifestID = errors.New("inner descriptor ID does not match computed hash")

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
	mCopy := *m
	mCopy.Descriptor.ID = core.ContentID{}
	return json.Marshal(&mCopy)
}

func Deserialize(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	rawInnerID := m.Descriptor.ID

	mCopy := m
	mCopy.Descriptor.ID = core.ContentID{}
	canonicalBytes, err := json.Marshal(&mCopy)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(canonicalBytes)
	var computedID core.ContentID
	copy(computedID[:], hash[:])

	if rawInnerID != (core.ContentID{}) && rawInnerID != computedID {
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInvalidManifestID, computedID, rawInnerID)
	}

	m.Descriptor.ID = computedID
	return &m, nil
}

