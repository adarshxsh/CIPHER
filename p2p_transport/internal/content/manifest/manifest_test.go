package manifest_test

import (
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

func TestManifest_ComputeDigest_Normalization(t *testing.T) {
	m1 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{1, 2, 3}, // Non-zero initial ID
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01, 0x02},
		},
		MerkleRoot: core.Hash{0xAA},
		WholeHash:  core.Hash{0xAA},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	m2 := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{9, 8, 7}, // Different initial ID
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{0x01, 0x02},
		},
		MerkleRoot: core.Hash{0xAA},
		WholeHash:  core.Hash{0xAA},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	digest1 := m1.ComputeDigest()
	digest2 := m2.ComputeDigest()

	if digest1 == (core.ContentID{}) {
		t.Fatalf("ComputeDigest returned zero digest")
	}

	if digest1 != digest2 {
		t.Fatalf("Manifests with identical content but different Descriptor.IDs produced different digests: %x vs %x", digest1, digest2)
	}

	// Verify ComputeDigestFromBytes
	m1.Descriptor.ID = digest1
	data, err := m1.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	digestFromBytes, err := manifest.ComputeDigestFromBytes(data)
	if err != nil {
		t.Fatalf("ComputeDigestFromBytes failed: %v", err)
	}

	if digestFromBytes != digest1 {
		t.Fatalf("ComputeDigestFromBytes (%x) != ComputeDigest (%x)", digestFromBytes, digest1)
	}
}

func TestManifest_ComputeDigest_DetectsTampering(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 2048,
		},
		ChunkIDs: []core.ChunkID{
			{0x01, 0x02},
		},
		MerkleRoot: core.Hash{0xBB},
		WholeHash:  core.Hash{0xBB},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	originalDigest := m.ComputeDigest()

	// Modify size
	mTamperedSize := *m
	mTamperedSize.Descriptor.Size = 4096
	if mTamperedSize.ComputeDigest() == originalDigest {
		t.Fatalf("Digest did not change after modifying size")
	}

	// Modify ChunkIDs
	mTamperedChunks := *m
	mTamperedChunks.ChunkIDs = []core.ChunkID{{0x99, 0x99}}
	if mTamperedChunks.ComputeDigest() == originalDigest {
		t.Fatalf("Digest did not change after modifying ChunkIDs")
	}

	// Modify Crypto Algorithm
	mTamperedCrypto := *m
	mTamperedCrypto.Crypto.Algorithm = "AES-GCM"
	if mTamperedCrypto.ComputeDigest() == originalDigest {
		t.Fatalf("Digest did not change after modifying crypto algorithm")
	}
}
