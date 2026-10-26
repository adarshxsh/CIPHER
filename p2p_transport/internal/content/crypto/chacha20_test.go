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

func TestChaCha20Encryptor_RandomNonceAndUniqueness(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	originalData := []byte("identical chunk content for nonce randomness test")

	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     42,
			PlainSize: uint32(len(originalData)),
		},
		Data: append([]byte(nil), originalData...),
	}

	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     42, // Same index
			PlainSize: uint32(len(originalData)),
		},
		Data: append([]byte(nil), originalData...),
	}

	if err := enc.EncryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to encrypt chunk1: %v", err)
	}

	if err := enc.EncryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to encrypt chunk2: %v", err)
	}

	// Verify nonces are 24 bytes (type array [24]byte)
	if len(chunk1.Header.Nonce) != 24 || len(chunk2.Header.Nonce) != 24 {
		t.Fatalf("expected 24-byte nonces, got %d and %d", len(chunk1.Header.Nonce), len(chunk2.Header.Nonce))
	}

	// Verify nonces are not zero
	zeroNonce := [24]byte{}
	if bytes.Equal(chunk1.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("chunk1 nonce is all zeros")
	}

	// Verify nonces across identical plaintexts and indices are distinct
	if bytes.Equal(chunk1.Header.Nonce[:], chunk2.Header.Nonce[:]) {
		t.Errorf("consecutive encryptions generated identical nonces")
	}

	// Verify ciphertexts are distinct
	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("consecutive encryptions generated identical ciphertexts")
	}
}
