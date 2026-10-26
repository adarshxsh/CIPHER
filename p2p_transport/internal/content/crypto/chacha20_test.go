package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
)

func TestChaCha20Encryptor_EncryptDecrypt(t *testing.T) {
	enc := NewChaCha20Encryptor()

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	originalData := []byte("hello decentralized encrypted cdn with xchacha20")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: uint32(len(originalData)),
		},
		Data: append([]byte(nil), originalData...),
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

	// Verify nonce is 24 bytes and non-zero
	var zeroNonce [24]byte
	if chunk.Header.Nonce == zeroNonce {
		t.Errorf("expected non-zero random nonce")
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
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	originalData := []byte("identical payload across multiple chunk encryptions")

	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0,
			PlainSize: uint32(len(originalData)),
		},
		Data: append([]byte(nil), originalData...),
	}

	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{
			Index:     0, // same index
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

	if chunk1.Header.Nonce == chunk2.Header.Nonce {
		t.Errorf("encrypting the same chunk twice produced identical nonces: %x", chunk1.Header.Nonce)
	}

	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("encrypting the same chunk twice produced identical ciphertexts")
	}
}

func TestChaCha20Encryptor_TamperedNonce(t *testing.T) {
	enc := NewChaCha20Encryptor()

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	originalData := []byte("tamper nonce authentication test payload")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: uint32(len(originalData)),
		},
		Data: append([]byte(nil), originalData...),
	}

	if err := enc.EncryptChunk(key, chunk); err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	// Tamper with the nonce
	chunk.Header.Nonce[0] ^= 0xFF

	err := enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption/authentication to fail for tampered nonce")
	}
}

func TestChaCha20Encryptor_Corruption(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: 5,
		},
		Data: []byte("hello"),
	}

	if err := enc.EncryptChunk(key, chunk); err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	// Corrupt ciphertext
	chunk.Data[0] ^= 0xFF

	err := enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail for corrupted ciphertext")
	}
}
