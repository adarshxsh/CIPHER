package manifest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"

	"cipher/internal/content/core"
)

var ErrContentIDMismatch = errors.New("content ID mismatch")

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

// ComputeContentID derives the ContentID by hashing the serialized canonical JSON bytes
// of the manifest with zeroed Descriptor.ID using SHA-256.
func (m *Manifest) ComputeContentID() (core.ContentID, error) {
	if m == nil {
		return core.ContentID{}, errors.New("nil manifest")
	}

	copyM := *m
	copyM.Descriptor.ID = core.ContentID{}
	if copyM.ChunkIDs == nil {
		copyM.ChunkIDs = []core.ChunkID{}
	}

	data, err := copyM.Serialize()
	if err != nil {
		return core.ContentID{}, err
	}

	hash := sha256.Sum256(data)
	var id core.ContentID
	copy(id[:], hash[:])
	return id, nil
}

