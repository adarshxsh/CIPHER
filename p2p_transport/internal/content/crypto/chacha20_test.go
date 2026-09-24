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

func TestChaCha20Encryptor_RandomNonce(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	originalData := []byte("confidential chunk payload")

	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0,
			PlainSize: uint32(len(originalData)),
		},
		Data: append([]byte(nil), originalData...),
	}

	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0, // Same index
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

	var zeroNonce [12]byte
	if bytes.Equal(chunk1.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("chunk1 header nonce was not populated")
	}
	if bytes.Equal(chunk2.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("chunk2 header nonce was not populated")
	}

	if bytes.Equal(chunk1.Header.Nonce[:], chunk2.Header.Nonce[:]) {
		t.Errorf("expected distinct nonces for identical keys and indices, but got identical nonces")
	}

	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("expected distinct ciphertexts when nonces differ, but got identical ciphertexts")
	}

	// Verify both chunks decrypt successfully
	if err := enc.DecryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to decrypt chunk1: %v", err)
	}
	if !bytes.Equal(chunk1.Data, originalData) {
		t.Errorf("chunk1 decrypted data mismatch")
	}

	if err := enc.DecryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to decrypt chunk2: %v", err)
	}
	if !bytes.Equal(chunk2.Data, originalData) {
		t.Errorf("chunk2 decrypted data mismatch")
	}
}

func TestChaCha20Encryptor_GenerateNonce(t *testing.T) {
	enc := NewChaCha20Encryptor()
	n1, err := enc.generateNonce()
	if err != nil {
		t.Fatalf("generateNonce error: %v", err)
	}
	if len(n1) != 12 {
		t.Fatalf("expected 12-byte nonce, got %d", len(n1))
	}

	n2, err := enc.generateNonce()
	if err != nil {
		t.Fatalf("generateNonce error: %v", err)
	}
	if bytes.Equal(n1, n2) {
		t.Errorf("consecutive generateNonce calls returned identical nonces")
	}
}
