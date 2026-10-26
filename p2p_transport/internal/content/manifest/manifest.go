package manifest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

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
	Version            uint16            `json:"version"`
	Descriptor         ContentDescriptor `json:"descriptor"`
	ChunkIDs           []core.ChunkID    `json:"chunk_ids"`
	MerkleRoot         core.Hash         `json:"merkle_root"` // Set to WholeHash for Milestone 7
	WholeHash          core.Hash         `json:"whole_hash"`
	Crypto             CryptoDescriptor  `json:"crypto"`
	PublisherPublicKey []byte            `json:"publisher_public_key,omitempty"`
	Signature          []byte            `json:"signature,omitempty"`
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

// SigningData constructs the canonical byte payload for signing from ContentDescriptor and MerkleRoot.
func (m *Manifest) SigningData() []byte {
	buf := new(bytes.Buffer)
	buf.Write(m.Descriptor.ID[:])
	buf.WriteString(string(m.Descriptor.Type))
	var sizeBuf [8]byte
	binary.BigEndian.PutUint64(sizeBuf[:], m.Descriptor.Size)
	buf.Write(sizeBuf[:])
	buf.Write(m.MerkleRoot[:])
	return buf.Bytes()
}

// Sign signs the manifest using the publisher's libp2p private key and sets PublisherPublicKey and Signature.
func (m *Manifest) Sign(privKey crypto.PrivKey) error {
	if privKey == nil {
		return errors.New("nil private key")
	}
	pubKey := privKey.GetPublic()
	pubBytes, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("failed to marshal publisher public key: %w", err)
	}
	sig, err := privKey.Sign(m.SigningData())
	if err != nil {
		return fmt.Errorf("failed to sign manifest data: %w", err)
	}
	m.PublisherPublicKey = pubBytes
	m.Signature = sig
	return nil
}

// VerifyPublisher cryptographically verifies the manifest signature using PublisherPublicKey.
func (m *Manifest) VerifyPublisher() error {
	if len(m.PublisherPublicKey) == 0 {
		return errors.New("missing publisher public key")
	}
	if len(m.Signature) == 0 {
		return errors.New("missing publisher signature")
	}
	pubKey, err := crypto.UnmarshalPublicKey(m.PublisherPublicKey)
	if err != nil {
		return fmt.Errorf("invalid publisher public key: %w", err)
	}
	valid, err := pubKey.Verify(m.SigningData(), m.Signature)
	if err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}
	if !valid {
		return errors.New("publisher signature verification failed: signature mismatch")
	}
	return nil
}

// GetPublisherPeerID returns the libp2p Peer ID derived from PublisherPublicKey.
func (m *Manifest) GetPublisherPeerID() (peer.ID, error) {
	if len(m.PublisherPublicKey) == 0 {
		return "", errors.New("missing publisher public key")
	}
	pubKey, err := crypto.UnmarshalPublicKey(m.PublisherPublicKey)
	if err != nil {
		return "", fmt.Errorf("invalid publisher public key: %w", err)
	}
	return peer.IDFromPublicKey(pubKey)
}

