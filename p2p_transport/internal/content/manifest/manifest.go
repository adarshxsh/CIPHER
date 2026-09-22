package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p/core/crypto"

	"cipher/internal/content/core"
)

var (
	ErrMissingPublisherSignature = errors.New("missing publisher public key or signature")
	ErrInvalidPublisherSignature = errors.New("publisher signature verification failed")

	pubKeyCache sync.Map // map[string]crypto.PubKey
)

// GetCachedPubKey returns an unmarshaled libp2p PubKey, caching the unmarshaled object to avoid redundant allocations.
func GetCachedPubKey(pubKeyBytes []byte) (crypto.PubKey, error) {
	if len(pubKeyBytes) == 0 {
		return nil, errors.New("empty public key bytes")
	}
	cacheKey := string(pubKeyBytes)
	if val, ok := pubKeyCache.Load(cacheKey); ok {
		return val.(crypto.PubKey), nil
	}
	pubKey, err := crypto.UnmarshalPublicKey(pubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal public key: %w", err)
	}
	pubKeyCache.Store(cacheKey, pubKey)
	return pubKey, nil
}

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

// Sign signs the manifest using the publisher's Ed25519 private key.
func (m *Manifest) Sign(privKey crypto.PrivKey) error {
	if privKey == nil {
		return errors.New("private key cannot be nil")
	}
	pubKey := privKey.GetPublic()
	pubBytes, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("failed to marshal public key: %w", err)
	}
	m.PublisherPubKey = pubBytes
	m.PublisherSignature = nil

	signable, err := m.SignableBytes()
	if err != nil {
		return fmt.Errorf("failed to compute signable bytes: %w", err)
	}

	sig, err := privKey.Sign(signable)
	if err != nil {
		return fmt.Errorf("failed to sign manifest: %w", err)
	}
	m.PublisherSignature = sig
	return nil
}

// SignableBytes returns the JSON bytes of the manifest omitting the PublisherSignature.
func (m *Manifest) SignableBytes() ([]byte, error) {
	mCopy := *m
	mCopy.PublisherSignature = nil
	return json.Marshal(&mCopy)
}

// VerifyPublisher verifies the cryptographic signature of the publisher on the manifest.
func (m *Manifest) VerifyPublisher() error {
	if len(m.PublisherPubKey) == 0 || len(m.PublisherSignature) == 0 {
		return ErrMissingPublisherSignature
	}

	pubKey, err := GetCachedPubKey(m.PublisherPubKey)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPublisherSignature, err)
	}

	signable, err := m.SignableBytes()
	if err != nil {
		return fmt.Errorf("failed to compute signable bytes: %w", err)
	}

	ok, err := pubKey.Verify(signable, m.PublisherSignature)
	if err != nil || !ok {
		return ErrInvalidPublisherSignature
	}
	return nil
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
