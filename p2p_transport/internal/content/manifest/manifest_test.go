package manifest_test

import (
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/identity"
)

func TestManifest_SignAndVerify(t *testing.T) {
	priv, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Size: 1024,
		},
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "ChaCha20-Poly1305",
		},
	}

	// Unsigned manifest verify should fail
	if err := m.VerifyPublisher(); !errors.Is(err, manifest.ErrMissingPublisherSignature) {
		t.Fatalf("Expected ErrMissingPublisherSignature, got %v", err)
	}

	// Sign manifest
	if err := m.Sign(priv); err != nil {
		t.Fatalf("Failed to sign manifest: %v", err)
	}

	if len(m.PublisherPubKey) == 0 || len(m.PublisherSignature) == 0 {
		t.Fatalf("Expected pubkey and signature to be set")
	}

	// Verification should pass
	if err := m.VerifyPublisher(); err != nil {
		t.Fatalf("Expected manifest publisher verification to pass, got: %v", err)
	}
}

func TestManifest_DetectsTampering(t *testing.T) {
	priv, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	var contentID core.ContentID
	contentID[0] = 0xFF

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Size: 2048,
		},
	}

	if err := m.Sign(priv); err != nil {
		t.Fatalf("Failed to sign: %v", err)
	}

	// Tamper descriptor size
	m.Descriptor.Size = 9999

	if err := m.VerifyPublisher(); !errors.Is(err, manifest.ErrInvalidPublisherSignature) {
		t.Fatalf("Expected ErrInvalidPublisherSignature on tampered manifest, got %v", err)
	}
}

func TestManifest_InvalidKey(t *testing.T) {
	priv1, _ := identity.GenerateEphemeral()
	priv2, _ := identity.GenerateEphemeral()

	m := &manifest.Manifest{Version: 1}
	m.Sign(priv1)

	// Replace pubkey with priv2's pubkey without updating signature
	pub2 := priv2.GetPublic()
	pub2Bytes, _ := crypto.MarshalPublicKey(pub2)
	m.PublisherPubKey = pub2Bytes

	if err := m.VerifyPublisher(); !errors.Is(err, manifest.ErrInvalidPublisherSignature) {
		t.Fatalf("Expected ErrInvalidPublisherSignature for pubkey mismatch, got %v", err)
	}
}
