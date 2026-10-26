package manifest

import (
	"bytes"
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

// Serialize returns the deterministic canonical JSON byte slice for the Manifest.
func (m *Manifest) Serialize() ([]byte, error) {
	return SerializeCanonical(m)
}

// SerializeCanonical serializes a Manifest into canonical JSON (sorted keys, compact spacing).
func SerializeCanonical(m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, errors.New("nil manifest")
	}
	mCopy := *m
	mCopy.Descriptor.ID = core.ContentID{}

	raw, err := json.Marshal(&mCopy)
	if err != nil {
		return nil, err
	}

	var generic interface{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}

	return json.Marshal(generic)
}

func Deserialize(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	h := sha256.Sum256(data)
	copy(m.Descriptor.ID[:], h[:])
	return &m, nil
}
