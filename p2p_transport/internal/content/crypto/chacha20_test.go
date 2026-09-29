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

func TestChaCha20Encryptor_RandomNonces(t *testing.T) {
	enc := NewChaCha20Encryptor()

	key := make([]byte, 32)
	rand.Read(key)

	originalData := []byte("identical payload for multiple encryptions")

	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{Index: 1, PlainSize: uint32(len(originalData))},
		Data:   append([]byte(nil), originalData...),
	}
	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{Index: 1, PlainSize: uint32(len(originalData))},
		Data:   append([]byte(nil), originalData...),
	}

	if err := enc.EncryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to encrypt chunk1: %v", err)
	}
	if err := enc.EncryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to encrypt chunk2: %v", err)
	}

	if bytes.Equal(chunk1.Header.Nonce[:], chunk2.Header.Nonce[:]) {
		t.Errorf("expected random nonces to differ across encryptions, but got identical nonces")
	}

	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("expected ciphertexts to differ due to distinct nonces, but got identical ciphertexts")
	}
}

func TestChaCha20Encryptor_NilChunk(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	if err := enc.EncryptChunk(key, nil); err == nil {
		t.Errorf("expected error when encrypting nil chunk, got nil")
	}

	if err := enc.DecryptChunk(key, nil); err == nil {
		t.Errorf("expected error when decrypting nil chunk, got nil")
	}
}

func TestChaCha20Encryptor_InvalidKey(t *testing.T) {
	enc := NewChaCha20Encryptor()
	badKey := []byte("short-key")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{PlainSize: 5},
		Data:   []byte("hello"),
	}

	if err := enc.EncryptChunk(badKey, chunk); err == nil {
		t.Errorf("expected error when encrypting with invalid key length, got nil")
	}

	if err := enc.DecryptChunk(badKey, chunk); err == nil {
		t.Errorf("expected error when decrypting with invalid key length, got nil")
	}
}

func TestChaCha20Encryptor_HeaderMismatches(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	originalData := []byte("hello world")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{PlainSize: uint32(len(originalData))},
		Data:   append([]byte(nil), originalData...),
	}

	if err := enc.EncryptChunk(key, chunk); err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	// CipherSize mismatch
	chunk.Header.CipherSize += 1
	if err := enc.DecryptChunk(key, chunk); err == nil {
		t.Errorf("expected error on CipherSize mismatch, got nil")
	}
	chunk.Header.CipherSize -= 1

	// PlainSize mismatch
	chunk.Header.PlainSize += 1
	if err := enc.DecryptChunk(key, chunk); err == nil {
		t.Errorf("expected error on PlainSize mismatch, got nil")
	}
	chunk.Header.PlainSize -= 1

	// Short ciphertext check
	shortChunk := &core.Chunk{
		Header: core.ChunkHeader{CipherSize: 5, PlainSize: 5},
		Data:   []byte("short"),
	}
	if err := enc.DecryptChunk(key, shortChunk); err == nil {
		t.Errorf("expected error when decrypting short ciphertext, got nil")
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

	// Corrupt ciphertext
	chunk.Data[0] ^= 0xFF

	err := enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail for corrupted ciphertext")
	}

	// Corrupt nonce
	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{PlainSize: 5},
		Data:   []byte("hello"),
	}
	enc.EncryptChunk(key, chunk2)
	chunk2.Header.Nonce[0] ^= 0xFF

	err2 := enc.DecryptChunk(key, chunk2)
	if err2 == nil {
		t.Errorf("expected decryption to fail for corrupted nonce")
	}
}
