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

func TestChaCha20Encryptor_RandomNonces(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	plaintext := []byte("identical plaintext payload for testing random nonces")

	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0,
			PlainSize: uint32(len(plaintext)),
		},
		Data: append([]byte(nil), plaintext...),
	}

	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0, // Same index
			PlainSize: uint32(len(plaintext)),
		},
		Data: append([]byte(nil), plaintext...),
	}

	if err := enc.EncryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to encrypt chunk 1: %v", err)
	}

	if err := enc.EncryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to encrypt chunk 2: %v", err)
	}

	// Verify 24-byte nonces are non-zero
	if len(chunk1.Header.Nonce) != 24 || len(chunk2.Header.Nonce) != 24 {
		t.Fatalf("expected 24-byte nonces")
	}

	var zeroNonce [24]byte
	if bytes.Equal(chunk1.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("chunk 1 nonce is zero")
	}
	if bytes.Equal(chunk2.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("chunk 2 nonce is zero")
	}

	// Verify nonces are different for identical chunks under the same key
	if bytes.Equal(chunk1.Header.Nonce[:], chunk2.Header.Nonce[:]) {
		t.Errorf("expected different random nonces for identical chunks, but nonces were identical")
	}

	// Verify ciphertexts are different
	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("expected different ciphertexts for identical chunks, but ciphertexts were identical")
	}

	// Verify both decrypt correctly
	if err := enc.DecryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to decrypt chunk 1: %v", err)
	}
	if err := enc.DecryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to decrypt chunk 2: %v", err)
	}

	if !bytes.Equal(chunk1.Data, plaintext) {
		t.Errorf("decrypted chunk 1 does not match original plaintext")
	}
	if !bytes.Equal(chunk2.Data, plaintext) {
		t.Errorf("decrypted chunk 2 does not match original plaintext")
	}
}
