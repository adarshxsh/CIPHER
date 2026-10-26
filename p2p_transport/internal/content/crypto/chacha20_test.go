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
		t.Fatalf("failed to read key: %v", err)
	}

	payload := []byte("identical payload for both chunks")

	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0,
			PlainSize: uint32(len(payload)),
		},
		Data: append([]byte(nil), payload...),
	}

	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0,
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

	if bytes.Equal(chunk1.Header.Nonce[:], chunk2.Header.Nonce[:]) {
		t.Errorf("expected nonces to be distinct for identical chunk index 0, got identical nonces: %x", chunk1.Header.Nonce)
	}

	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("expected ciphertexts to be distinct when nonces differ")
	}

	// Verify both decrypt correctly
	if err := enc.DecryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to decrypt chunk1: %v", err)
	}
	if !bytes.Equal(chunk1.Data, payload) {
		t.Errorf("chunk1 decrypted payload mismatch")
	}

	if err := enc.DecryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to decrypt chunk2: %v", err)
	}
	if !bytes.Equal(chunk2.Data, payload) {
		t.Errorf("chunk2 decrypted payload mismatch")
	}
}

func TestChaCha20Encryptor_Errors(t *testing.T) {
	enc := NewChaCha20Encryptor()
	invalidKey := []byte("short_key")
	validKey := make([]byte, 32)

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: 5,
		},
		Data: []byte("hello"),
	}

	// Encrypt with invalid key length
	if err := enc.EncryptChunk(invalidKey, chunk); err == nil {
		t.Errorf("expected error when encrypting with invalid key length")
	}

	// Decrypt with invalid key length
	if err := enc.DecryptChunk(invalidKey, chunk); err == nil {
		t.Errorf("expected error when decrypting with invalid key length")
	}

	// Encrypt validly first
	chunk.Header.PlainSize = 5
	chunk.Data = []byte("hello")
	if err := enc.EncryptChunk(validKey, chunk); err != nil {
		t.Fatalf("failed to encrypt chunk: %v", err)
	}

	// Cipher size mismatch error
	chunkCopy := *chunk
	chunkCopy.Header.CipherSize = chunkCopy.Header.CipherSize + 1
	if err := enc.DecryptChunk(validKey, &chunkCopy); err == nil {
		t.Errorf("expected error on cipher size mismatch")
	}

	// Plain size mismatch error
	chunkCopy2 := *chunk
	chunkCopy2.Header.PlainSize = chunkCopy2.Header.PlainSize + 1
	if err := enc.DecryptChunk(validKey, &chunkCopy2); err == nil {
		t.Errorf("expected error on plain size mismatch")
	}
}
