package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"

	"cipher/internal/content/core"
)

const (
	MaxManifestJSONSize = 2097117
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
	Version            uint16            `json:"version"`
	Descriptor         ContentDescriptor `json:"descriptor"`
	ChunkIDs           []core.ChunkID    `json:"chunk_ids"`
	MerkleRoot         core.Hash         `json:"merkle_root"` // Set to WholeHash for Milestone 7
	WholeHash          core.Hash         `json:"whole_hash"`
	Crypto             CryptoDescriptor  `json:"crypto"`
	PublisherPubKey    []byte            `json:"publisher_pub_key,omitempty"`
	PublisherSignature []byte            `json:"publisher_signature,omitempty"`
}

// UserMetadata represents mutable metadata not essential to the content's integrity.
type UserMetadata struct {
	Filename  string `json:"filename"`
	MimeType  string `json:"mime_type"`
	CreatedAt int64  `json:"created_at"`
}

func (m *Manifest) SignableData() []byte {
	buf := new(bytes.Buffer)
	buf.WriteString("CIPHER-PUBLISHER-MANIFEST:")
	buf.Write(m.Descriptor.ID[:])
	buf.Write(m.WholeHash[:])
	return buf.Bytes()
}

func (m *Manifest) SignPublisher(privKey crypto.PrivKey) error {
	if privKey == nil {
		return errors.New("private key is nil")
	}
	pubKeyBytes, err := crypto.MarshalPublicKey(privKey.GetPublic())
	if err != nil {
		return fmt.Errorf("failed to marshal publisher public key: %w", err)
	}
	m.PublisherPubKey = pubKeyBytes
	data := m.SignableData()
	sig, err := privKey.Sign(data)
	if err != nil {
		return fmt.Errorf("failed to sign manifest as publisher: %w", err)
	}
	m.PublisherSignature = sig
	return nil
}

func (m *Manifest) VerifyPublisher() error {
	if len(m.PublisherPubKey) == 0 {
		return nil
	}
	if len(m.PublisherSignature) == 0 {
		return errors.New("publisher public key present but missing signature")
	}
	pubKey, err := crypto.UnmarshalPublicKey(m.PublisherPubKey)
	if err != nil {
		return fmt.Errorf("invalid publisher public key: %w", err)
	}
	data := m.SignableData()
	ok, err := pubKey.Verify(data, m.PublisherSignature)
	if err != nil {
		return fmt.Errorf("publisher signature verification error: %w", err)
	}
	if !ok {
		return errors.New("publisher signature verification failed")
	}
	return nil
}

func (m *Manifest) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func Deserialize(data []byte) (*Manifest, error) {
	if len(data) > MaxManifestJSONSize {
		return nil, errors.New("manifest JSON size exceeds maximum allowed limit")
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if err := m.VerifyPublisher(); err != nil {
		return nil, fmt.Errorf("publisher signature verification failed: %w", err)
	}
	return &m, nil
}
