package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

func TestChaCha20Encryptor(t *testing.T) {
	enc := NewChaCha20Encryptor()

	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

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

	// Expected encrypted length = 24 (nonce) + len(originalData) + 16 (poly1305 tag)
	expectedCipherLen := chacha20poly1305.NonceSizeX + len(originalData) + chacha20poly1305.Overhead
	if len(chunk.Data) != expectedCipherLen {
		t.Errorf("expected ciphertext length %d, got %d", expectedCipherLen, len(chunk.Data))
	}

	if chunk.Header.CipherSize != uint32(len(chunk.Data)) {
		t.Errorf("expected CipherSize to match Data length")
	}

	// Verify the 24-byte nonce prepended to chunk.Data matches chunk.Header.Nonce
	if !bytes.Equal(chunk.Data[:chacha20poly1305.NonceSizeX], chunk.Header.Nonce[:]) {
		t.Errorf("prepended nonce in chunk.Data does not match chunk.Header.Nonce")
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
	key := make([]byte, chacha20poly1305.KeySize)
	rand.Read(key)

	nonces := make(map[string]bool)
	numIterations := 100

	for i := 0; i < numIterations; i++ {
		chunk := &core.Chunk{
			Header: core.ChunkHeader{
				PlainSize: 12,
				Index:     0, // same chunk index for all
			},
			Data: []byte("repeat chunk"),
		}

		if err := enc.EncryptChunk(key, chunk); err != nil {
			t.Fatalf("failed to encrypt at iteration %d: %v", i, err)
		}

		nonceStr := string(chunk.Header.Nonce[:])
		if nonces[nonceStr] {
			t.Fatalf("duplicate nonce generated on iteration %d", i)
		}
		nonces[nonceStr] = true

		// Check prepended nonce in chunk.Data matches header nonce
		if !bytes.Equal(chunk.Data[:chacha20poly1305.NonceSizeX], chunk.Header.Nonce[:]) {
			t.Fatalf("prepended nonce mismatch at iteration %d", i)
		}
	}
}

func TestChaCha20Encryptor_Corruption(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, chacha20poly1305.KeySize)
	rand.Read(key)

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: 5,
		},
		Data: []byte("hello"),
	}

	if err := enc.EncryptChunk(key, chunk); err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Corrupt ciphertext body
	chunk.Data[chacha20poly1305.NonceSizeX+1] ^= 0xFF

	err := enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail for corrupted ciphertext")
	}
}

func TestChaCha20Encryptor_ShortCiphertext(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, chacha20poly1305.KeySize)
	rand.Read(key)

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			CipherSize: 10,
		},
		Data: make([]byte, 10), // less than NonceSizeX (24) + Overhead (16)
	}

	err := enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail for short ciphertext payload")
	}
}
