package manifest

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"

	"cipher/internal/content/core"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
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
	PublicKey  []byte            `json:"public_key,omitempty"`
	Signature  []byte            `json:"signature,omitempty"`
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

// GetSignableBytes returns the canonical JSON byte representation of the Manifest with Signature omitted.
func (m *Manifest) GetSignableBytes() ([]byte, error) {
	mCopy := *m
	mCopy.Signature = nil
	return json.Marshal(&mCopy)
}

// Sign signs the signable manifest representation using the provided Ed25519 private key.
func (m *Manifest) Sign(priv interface{}) error {
	if priv == nil {
		return errors.New("nil private key provided")
	}

	var pubBytes []byte
	var sigBytes []byte

	switch k := priv.(type) {
	case libp2pcrypto.PrivKey:
		pub := k.GetPublic()
		rawPub, err := pub.Raw()
		if err != nil {
			return fmt.Errorf("failed to extract raw public key: %w", err)
		}
		pubBytes = rawPub
		m.PublicKey = pubBytes
		m.Signature = nil

		signableBytes, err := m.GetSignableBytes()
		if err != nil {
			return fmt.Errorf("failed to get signable bytes: %w", err)
		}

		sig, err := k.Sign(signableBytes)
		if err != nil {
			return fmt.Errorf("failed to sign manifest bytes: %w", err)
		}
		sigBytes = sig

	case ed25519.PrivateKey:
		pubKey := k.Public().(ed25519.PublicKey)
		pubBytes = []byte(pubKey)
		m.PublicKey = pubBytes
		m.Signature = nil

		signableBytes, err := m.GetSignableBytes()
		if err != nil {
			return fmt.Errorf("failed to get signable bytes: %w", err)
		}

		sigBytes = ed25519.Sign(k, signableBytes)

	default:
		return fmt.Errorf("unsupported key type: %T", priv)
	}

	m.Signature = sigBytes
	return nil
}

// Verify returns true if the manifest contains a valid digital signature corresponding to PublicKey.
func (m *Manifest) Verify() bool {
	if len(m.PublicKey) == 0 || len(m.Signature) == 0 {
		return false
	}

	signableBytes, err := m.GetSignableBytes()
	if err != nil {
		return false
	}

	if len(m.PublicKey) == ed25519.PublicKeySize {
		return ed25519.Verify(ed25519.PublicKey(m.PublicKey), signableBytes, m.Signature)
	}

	pubKey, err := libp2pcrypto.UnmarshalPublicKey(m.PublicKey)
	if err != nil {
		return false
	}

	valid, err := pubKey.Verify(signableBytes, m.Signature)
	return err == nil && valid
}

// VerifyPublisher is an alias for Verify to ensure backward/forward compatibility.
func (m *Manifest) VerifyPublisher() bool {
	return m.Verify()
}

