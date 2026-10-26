package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
)

func TestXChaCha20Encryptor(t *testing.T) {
	enc := NewXChaCha20Encryptor()

	key := make([]byte, 32)
	rand.Read(key)

	originalData := []byte("hello decentralized encrypted cdn")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: uint32(len(originalData)),
		},
		Data: append([]byte(nil), originalData...), // copy
	}

	// Encrypt
	if err := enc.EncryptChunk(key, chunk); err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	if chunk.Header.CipherSize != uint32(len(chunk.Data)) {
		t.Errorf("expected CipherSize to match Data length")
	}

	if bytes.Equal(chunk.Data, originalData) {
		t.Errorf("ciphertext is identical to plaintext")
	}

	if len(chunk.Header.Nonce) != 24 {
		t.Errorf("expected 24-byte nonce, got %d", len(chunk.Header.Nonce))
	}

	// Decrypt
	if err := enc.DecryptChunk(key, chunk); err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if chunk.Header.PlainSize != uint32(len(chunk.Data)) {
		t.Errorf("expected PlainSize to match Data length")
	}

	if !bytes.Equal(chunk.Data, originalData) {
		t.Errorf("decrypted data %q doesn't match original %q", chunk.Data, originalData)
	}
}

func TestXChaCha20Encryptor_RandomNoncesAndNoCollisions(t *testing.T) {
	enc := NewXChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	const numChunks = 100
	seenNonces := make(map[[24]byte]bool)

	for i := 0; i < numChunks; i++ {
		chunk := &core.Chunk{
			Header: core.ChunkHeader{
				Index:     0, // same chunk index across all runs
				PlainSize: 5,
			},
			Data: []byte("hello"),
		}

		if err := enc.EncryptChunk(key, chunk); err != nil {
			t.Fatalf("failed to encrypt chunk %d: %v", i, err)
		}

		nonce := chunk.Header.Nonce
		if seenNonces[nonce] {
			t.Fatalf("nonce collision detected on chunk %d: %x", i, nonce)
		}
		seenNonces[nonce] = true
	}
}

func TestXChaCha20Encryptor_Corruption(t *testing.T) {
	enc := NewXChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: 5,
		},
		Data: []byte("hello"),
	}

	if err := enc.EncryptChunk(key, chunk); err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	// Corrupt
	chunk.Data[0] ^= 0xFF

	err := enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail for corrupted ciphertext")
	}
}
