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
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate random key: %v", err)
	}

	payload := []byte("identical payload for same chunk index")
	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     42,
			PlainSize: uint32(len(payload)),
		},
		Data: append([]byte(nil), payload...),
	}
	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     42, // Same index!
			PlainSize: uint32(len(payload)),
		},
		Data: append([]byte(nil), payload...),
	}

	if err := enc.EncryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to encrypt chunk1: %v", err)
	}

	if err := enc.EncryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to encrypt chunk2: %v", err)
	}

	// Nonces must be unique (not zero, and chunk1.Nonce != chunk2.Nonce)
	var zeroNonce [12]byte
	if bytes.Equal(chunk1.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("chunk1 nonce is all zeroes")
	}
	if bytes.Equal(chunk2.Header.Nonce[:], zeroNonce[:]) {
		t.Errorf("chunk2 nonce is all zeroes")
	}
	if bytes.Equal(chunk1.Header.Nonce[:], chunk2.Header.Nonce[:]) {
		t.Errorf("expected random nonces, but chunk1 and chunk2 nonces are identical: %x", chunk1.Header.Nonce)
	}

	// Ciphertexts must be different because nonces are different
	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("expected different ciphertexts for identical plaintext encrypted with different nonces")
	}

	// Both chunks must decrypt correctly
	if err := enc.DecryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to decrypt chunk1: %v", err)
	}
	if err := enc.DecryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to decrypt chunk2: %v", err)
	}

	if !bytes.Equal(chunk1.Data, payload) || !bytes.Equal(chunk2.Data, payload) {
		t.Errorf("decrypted payload mismatch")
	}
}

