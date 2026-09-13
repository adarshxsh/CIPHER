package manifest_test

import (
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func createSampleManifest() *manifest.Manifest {
	var cid core.ContentID
	copy(cid[:], []byte("sample_content_id_1234567890123"))

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("sample_chunk_id_12345678901234"))

	var hash core.Hash
	copy(hash[:], []byte("sample_whole_hash_123456789012"))

	return &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs:   []core.ChunkID{chunkID},
		MerkleRoot: hash,
		WholeHash:  hash,
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}
}

func TestManifestSignAndVerify(t *testing.T) {
	priv, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	m := createSampleManifest()

	// Before signing, VerifySignature must fail
	if err := m.VerifySignature(); err == nil {
		t.Fatalf("expected error on unsigned manifest, got nil")
	}

	// Sign manifest
	if err := m.Sign(priv); err != nil {
		t.Fatalf("failed to sign manifest: %v", err)
	}

	// Verify signature
	if err := m.VerifySignature(); err != nil {
		t.Fatalf("VerifySignature failed: %v", err)
	}

	// Verify PublisherPeerID
	expectedPeerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get expected peer ID: %v", err)
	}

	actualPeerID, err := m.PublisherPeerID()
	if err != nil {
		t.Fatalf("PublisherPeerID failed: %v", err)
	}

	if expectedPeerID != actualPeerID {
		t.Fatalf("publisher peer ID mismatch: expected %s, got %s", expectedPeerID, actualPeerID)
	}
}

func TestManifestTamperedPayloadFailsVerification(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	m := createSampleManifest()
	if err := m.Sign(priv); err != nil {
		t.Fatalf("failed to sign manifest: %v", err)
	}

	// Tamper with manifest content
	m.Descriptor.Size = 2048

	if err := m.VerifySignature(); err == nil {
		t.Fatalf("expected signature verification failure on tampered manifest, got nil")
	}
}

func TestManifestSerializationAndDeserialization(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	m := createSampleManifest()
	if err := m.Sign(priv); err != nil {
		t.Fatalf("failed to sign manifest: %v", err)
	}

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	deserialized, err := manifest.Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize manifest: %v", err)
	}

	if err := deserialized.VerifySignature(); err != nil {
		t.Fatalf("deserialized manifest signature verification failed: %v", err)
	}
}

func BenchmarkManifestSignatureVerification(b *testing.B) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		b.Fatalf("failed to generate key pair: %v", err)
	}

	m := createSampleManifest()
	if err := m.Sign(priv); err != nil {
		b.Fatalf("failed to sign manifest: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.VerifySignature(); err != nil {
			b.Fatalf("VerifySignature failed: %v", err)
		}
	}
}
