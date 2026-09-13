package manifest

import (
	"encoding/json"
	"fmt"

	"cipher/internal/content/core"
	"cipher/internal/discovery"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
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

// SignatureDescriptor holds publisher signature metadata.
type SignatureDescriptor struct {
	PublisherPublicKey []byte `json:"publisher_public_key"`
	Algorithm          string `json:"algorithm"`
	Signature          []byte `json:"signature"`
}

// Manifest represents the capability to understand the immutable content.
type Manifest struct {
	Version     uint16                 `json:"version"`
	Descriptor  ContentDescriptor      `json:"descriptor"`
	ChunkIDs    []core.ChunkID         `json:"chunk_ids"`
	MerkleRoot  core.Hash              `json:"merkle_root"` // Set to WholeHash for Milestone 7
	WholeHash   core.Hash              `json:"whole_hash"`
	Crypto      CryptoDescriptor       `json:"crypto"`
	Signature   *SignatureDescriptor   `json:"signature,omitempty"`
	Attestation *discovery.Attestation `json:"attestation,omitempty"`
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

// SigningBytes returns the canonical JSON byte representation of the manifest payload excluding signature and attestation.
func (m *Manifest) SigningBytes() ([]byte, error) {
	mCopy := *m
	mCopy.Signature = nil
	mCopy.Attestation = nil
	return json.Marshal(&mCopy)
}

// Sign signs the manifest payload using the publisher's Ed25519 private key.
func (m *Manifest) Sign(privKey crypto.PrivKey) error {
	if privKey == nil {
		return fmt.Errorf("private key is required for signing manifest")
	}
	payloadBytes, err := m.SigningBytes()
	if err != nil {
		return fmt.Errorf("failed to generate signing bytes: %w", err)
	}
	sig, err := privKey.Sign(payloadBytes)
	if err != nil {
		return fmt.Errorf("failed to sign manifest payload: %w", err)
	}
	pubBytes, err := crypto.MarshalPublicKey(privKey.GetPublic())
	if err != nil {
		return fmt.Errorf("failed to marshal public key: %w", err)
	}

	m.Signature = &SignatureDescriptor{
		PublisherPublicKey: pubBytes,
		Algorithm:          "Ed25519",
		Signature:          sig,
	}
	return nil
}

// VerifySignature cryptographically validates the manifest signature against the embedded publisher public key.
func (m *Manifest) VerifySignature() error {
	if m.Signature == nil {
		return fmt.Errorf("manifest missing cryptographic signature")
	}
	if len(m.Signature.PublisherPublicKey) == 0 {
		return fmt.Errorf("manifest signature missing publisher public key")
	}
	if len(m.Signature.Signature) == 0 {
		return fmt.Errorf("manifest signature missing payload signature")
	}
	if m.Signature.Algorithm != "Ed25519" && m.Signature.Algorithm != "" {
		return fmt.Errorf("unsupported signature algorithm: %s", m.Signature.Algorithm)
	}

	pubKey, err := crypto.UnmarshalPublicKey(m.Signature.PublisherPublicKey)
	if err != nil {
		return fmt.Errorf("failed to unmarshal publisher public key: %w", err)
	}

	payloadBytes, err := m.SigningBytes()
	if err != nil {
		return fmt.Errorf("failed to generate signing bytes for verification: %w", err)
	}

	valid, err := pubKey.Verify(payloadBytes, m.Signature.Signature)
	if err != nil {
		return fmt.Errorf("failed to verify manifest signature: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid manifest cryptographic signature")
	}

	return nil
}

// PublisherPublicKey extracts and unmarshals the libp2p PubKey of the publisher.
func (m *Manifest) PublisherPublicKey() (crypto.PubKey, error) {
	if m.Signature == nil || len(m.Signature.PublisherPublicKey) == 0 {
		return nil, fmt.Errorf("manifest signature missing publisher public key")
	}
	return crypto.UnmarshalPublicKey(m.Signature.PublisherPublicKey)
}

// PublisherPeerID derives the libp2p peer.ID of the publisher from the signature's public key.
func (m *Manifest) PublisherPeerID() (peer.ID, error) {
	pubKey, err := m.PublisherPublicKey()
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(pubKey)
}
