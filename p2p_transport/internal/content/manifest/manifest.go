package manifest

import (
	"encoding/json"
	
	"github.com/libp2p/go-libp2p/core/crypto"
	
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
	Publisher  []byte            `json:"publisher,omitempty"`
	Signature  []byte            `json:"signature,omitempty"`
}

// UserMetadata represents mutable metadata not essential to the content's integrity.
type UserMetadata struct {
	Filename  string `json:"filename"`
	MimeType  string `json:"mime_type"`
	CreatedAt int64  `json:"created_at"`
}

func (m *Manifest) Sign(priv crypto.PrivKey) error {
	m.Publisher = nil
	m.Signature = nil

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	sig, err := priv.Sign(data)
	if err != nil {
		return err
	}

	pubBytes, err := crypto.MarshalPublicKey(priv.GetPublic())
	if err != nil {
		return err
	}

	m.Publisher = pubBytes
	m.Signature = sig
	return nil
}

func (m *Manifest) Verify() (bool, error) {
	if len(m.Publisher) == 0 || len(m.Signature) == 0 {
		return false, nil
	}

	pub, err := crypto.UnmarshalPublicKey(m.Publisher)
	if err != nil {
		return false, err
	}

	sig := m.Signature
	pubBytes := m.Publisher

	m.Publisher = nil
	m.Signature = nil
	data, err := json.Marshal(m)

	m.Publisher = pubBytes
	m.Signature = sig

	if err != nil {
		return false, err
	}

	return pub.Verify(data, sig)
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
