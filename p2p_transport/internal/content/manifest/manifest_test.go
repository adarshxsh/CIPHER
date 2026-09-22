package manifest_test

import (
	"testing"

	"cipher/internal/content/manifest"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestManifestSignAndVerify(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
	}

	// Unsigned manifest verify should return false, nil
	valid, err := m.Verify()
	if err != nil {
		t.Fatalf("unexpected error verifying unsigned manifest: %v", err)
	}
	if valid {
		t.Fatalf("expected unsigned manifest to fail verification")
	}

	// Sign manifest
	if err := m.Sign(priv); err != nil {
		t.Fatalf("failed to sign manifest: %v", err)
	}

	if len(m.Publisher) == 0 || len(m.Signature) == 0 {
		t.Fatalf("expected non-empty publisher and signature after signing")
	}

	// Verify signed manifest
	valid, err = m.Verify()
	if err != nil {
		t.Fatalf("unexpected error verifying signed manifest: %v", err)
	}
	if !valid {
		t.Fatalf("expected signed manifest to be valid")
	}

	// Tamper signature
	m.Signature[0] ^= 0xff
	valid, err = m.Verify()
	if err != nil {
		t.Fatalf("unexpected error verifying tampered manifest: %v", err)
	}
	if valid {
		t.Fatalf("expected tampered manifest signature to fail verification")
	}
}
