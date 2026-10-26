package manifest_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
)

func TestManifestSignAndVerify_Ed25519(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ed25519 key: %v", err)
	}

	var cid core.ContentID
	cid[0] = 0x12

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 1024,
		},
	}

	if err := m.Sign(priv); err != nil {
		t.Fatalf("failed to sign manifest: %v", err)
	}

	if len(m.PublicKey) == 0 || len(m.Signature) == 0 {
		t.Fatalf("expected PublicKey and Signature to be populated")
	}

	if !m.Verify() {
		t.Fatalf("expected manifest verification to pass")
	}

	if !m.VerifyPublisher() {
		t.Fatalf("expected VerifyPublisher to pass")
	}

	// Test Serialization roundtrip
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	if !deserialized.Verify() {
		t.Fatalf("deserialized manifest verification failed")
	}

	_ = pub
}

func TestManifestSignAndVerify_Libp2pKey(t *testing.T) {
	priv, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate libp2p key: %v", err)
	}

	var cid core.ContentID
	cid[0] = 0x34

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 2048,
		},
	}

	if err := m.Sign(priv); err != nil {
		t.Fatalf("failed to sign manifest: %v", err)
	}

	if !m.Verify() {
		t.Fatalf("expected manifest verification to pass for libp2p key")
	}
}

func TestManifestVerify_RejectsTamperedManifest(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	var cid core.ContentID
	cid[0] = 0xAA

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 100,
		},
	}

	if err := m.Sign(priv); err != nil {
		t.Fatalf("failed to sign manifest: %v", err)
	}

	// Tamper with Descriptor ID
	m.Descriptor.ID[0] = 0xBB
	if m.Verify() {
		t.Fatalf("expected verification to fail after tampering with ID")
	}

	// Restore ID and tamper with Size
	m.Descriptor.ID[0] = 0xAA
	m.Descriptor.Size = 200
	if m.Verify() {
		t.Fatalf("expected verification to fail after tampering with Size")
	}

	// Tamper with signature
	m.Descriptor.Size = 100
	m.Signature[0] ^= 0xFF
	if m.Verify() {
		t.Fatalf("expected verification to fail with corrupted signature")
	}
}

func TestManifestVerify_RejectsMissingSignature(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
	}

	if m.Verify() {
		t.Fatalf("expected verification to fail when signature is missing")
	}

	m.PublicKey = []byte("dummy")
	if m.Verify() {
		t.Fatalf("expected verification to fail when signature is empty")
	}
}
