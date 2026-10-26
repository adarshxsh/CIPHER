package manifest_test

import (
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestManifest_SignAndVerify(t *testing.T) {
	priv, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	var merkleRoot core.Hash
	copy(merkleRoot[:], []byte("12345678901234567890123456789012"))

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		MerkleRoot: merkleRoot,
		WholeHash:  merkleRoot,
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "ChaCha20-Poly1305",
			Version:   1,
		},
	}

	// Unsigned manifest verify should fail
	if err := m.VerifyPublisher(); err == nil {
		t.Fatalf("Expected error verifying unsigned manifest, got nil")
	}

	// Sign manifest
	if err := m.Sign(priv); err != nil {
		t.Fatalf("Failed to sign manifest: %v", err)
	}

	if len(m.PublisherPublicKey) == 0 {
		t.Fatalf("PublisherPublicKey is empty")
	}
	if len(m.Signature) == 0 {
		t.Fatalf("Signature is empty")
	}

	// Verify valid signature
	if err := m.VerifyPublisher(); err != nil {
		t.Fatalf("VerifyPublisher failed on valid manifest: %v", err)
	}

	// Verify peer ID matches public key
	peerID, err := m.GetPublisherPeerID()
	if err != nil {
		t.Fatalf("GetPublisherPeerID failed: %v", err)
	}
	if !peerID.MatchesPublicKey(pub) {
		t.Fatalf("Peer ID derived from manifest does not match pub key")
	}

	// Test Serialization & Deserialization
	serialized, err := m.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize manifest: %v", err)
	}

	m2, err := manifest.Deserialize(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	if err := m2.VerifyPublisher(); err != nil {
		t.Fatalf("VerifyPublisher failed on deserialized manifest: %v", err)
	}
}

func TestManifest_TamperedPayload(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Type: manifest.TypeFile,
			Size: 1024,
		},
	}

	if err := m.Sign(priv); err != nil {
		t.Fatalf("Failed to sign manifest: %v", err)
	}

	// Tamper descriptor size
	m.Descriptor.Size = 2048
	if err := m.VerifyPublisher(); err == nil {
		t.Fatalf("Expected verification failure after tampering descriptor size")
	}

	// Reset size and tamper signature
	m.Descriptor.Size = 1024
	m.Signature[0] ^= 0xFF
	if err := m.VerifyPublisher(); err == nil {
		t.Fatalf("Expected verification failure with corrupted signature")
	}
}

func TestManifest_InvalidPublicKey(t *testing.T) {
	priv, _, _ := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	m := &manifest.Manifest{Version: 1}
	_ = m.Sign(priv)

	m.PublisherPublicKey = []byte("corrupted_pub_key")
	if err := m.VerifyPublisher(); err == nil {
		t.Fatalf("Expected error when verifying with corrupted public key")
	}
}
