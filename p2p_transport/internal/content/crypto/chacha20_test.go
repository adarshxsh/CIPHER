package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
)

func TestChaCha20Encryptor(t *testing.T) {
	enc := NewChaCha20Encryptor()

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

	// Verify 24-byte nonce length and non-zero content
	if len(chunk.Header.Nonce) != 24 {
		t.Fatalf("expected 24-byte nonce, got %d bytes", len(chunk.Header.Nonce))
	}
	var zeroNonce [24]byte
	if bytes.Equal(chunk.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("expected non-zero random nonce")
	}

	if chunk.Header.CipherSize != uint32(len(chunk.Data)) {
		t.Errorf("expected CipherSize to match Data length")
	}

	if bytes.Equal(chunk.Data, originalData) {
		t.Errorf("ciphertext is identical to plaintext")
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

func TestChaCha20Encryptor_RandomNonceUniqueness(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	seenNonces := make(map[[24]byte]bool)
	const count = 1000

	for i := 0; i < count; i++ {
		chunk := &core.Chunk{
			Header: core.ChunkHeader{
				Index:     0, // Identical chunk index
				PlainSize: 11,
			},
			Data: []byte("repeat data"),
		}

		if err := enc.EncryptChunk(key, chunk); err != nil {
			t.Fatalf("failed to encrypt chunk at iteration %d: %v", i, err)
		}

		nonce := chunk.Header.Nonce
		if seenNonces[nonce] {
			t.Fatalf("nonce collision detected at iteration %d: %x", i, nonce)
		}
		seenNonces[nonce] = true
	}
}

func TestChaCha20Encryptor_Corruption(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: 5,
		},
		Data: []byte("hello"),
	}

	enc.EncryptChunk(key, chunk)

	// Corrupt
	chunk.Data[0] ^= 0xFF

	err := enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail for corrupted ciphertext")
	}
}
