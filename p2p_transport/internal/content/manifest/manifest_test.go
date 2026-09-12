package manifest_test

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestPublisherSignature_Valid(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x01, 0x02},
			Type: manifest.TypeFile,
			Size: 100,
		},
		WholeHash: core.Hash{0xAA, 0xBB},
	}

	if err := m.SignPublisher(priv); err != nil {
		t.Fatalf("SignPublisher failed: %v", err)
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	parsed, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(parsed.PublisherPubKey) == 0 {
		t.Errorf("expected non-empty PublisherPubKey")
	}
	if len(parsed.PublisherSignature) == 0 {
		t.Errorf("expected non-empty PublisherSignature")
	}
}

func TestPublisherSignature_Tampered(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x01, 0x02},
			Type: manifest.TypeFile,
			Size: 100,
		},
		WholeHash: core.Hash{0xAA, 0xBB},
	}

	if err := m.SignPublisher(priv); err != nil {
		t.Fatalf("SignPublisher failed: %v", err)
	}

	// Corrupt signature byte
	m.PublisherSignature[0] ^= 0xFF

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	_, err = manifest.Deserialize(data)
	if err == nil {
		t.Fatalf("expected Deserialize to fail on tampered publisher signature, got nil")
	}
}

func TestManifest_ExceedsMaxJSONSize(t *testing.T) {
	hugeData := make([]byte, manifest.MaxManifestJSONSize+10)
	for i := range hugeData {
		hugeData[i] = ' '
	}

	_, err := manifest.Deserialize(hugeData)
	if err == nil {
		t.Fatalf("expected error for manifest exceeding MaxManifestJSONSize, got nil")
	}
}
