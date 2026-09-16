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

func TestChaCha20Encryptor_SessionIVInitialization(t *testing.T) {
	enc1 := NewChaCha20Encryptor()
	enc2 := NewChaCha20Encryptor()

	iv1 := enc1.SessionIV()
	iv2 := enc2.SessionIV()

	if len(iv1) != SessionIVSize {
		t.Fatalf("expected session IV length %d, got %d", SessionIVSize, len(iv1))
	}
	if len(iv2) != SessionIVSize {
		t.Fatalf("expected session IV length %d, got %d", SessionIVSize, len(iv2))
	}

	if bytes.Equal(iv1, iv2) {
		t.Errorf("expected distinct session IVs for enc1 and enc2, got identical IVs")
	}
}

func TestGenerateNonce_DistinctAcrossSessions(t *testing.T) {
	iv1 := []byte("sessionIV-one")
	iv2 := []byte("sessionIV-two")
	const chunkIndex = 42

	nonce1 := GenerateNonce(iv1, chunkIndex)
	nonce2 := GenerateNonce(iv2, chunkIndex)

	if len(nonce1) != 12 || len(nonce2) != 12 {
		t.Fatalf("expected 12-byte nonces, got len %d and %d", len(nonce1), len(nonce2))
	}

	if bytes.Equal(nonce1, nonce2) {
		t.Errorf("expected distinct nonces for identical chunk indices across different sessions, got identical: %x", nonce1)
	}

	// Also test via ChaCha20Encryptor instances
	enc1 := NewChaCha20Encryptor()
	enc2 := NewChaCha20Encryptor()
	eNonce1 := enc1.GenerateNonce(chunkIndex)
	eNonce2 := enc2.GenerateNonce(chunkIndex)

	if bytes.Equal(eNonce1, eNonce2) {
		t.Errorf("expected distinct nonces from different ChaCha20Encryptor instances, got identical: %x", eNonce1)
	}
}

func TestGenerateNonce_DistinctAcrossIndices(t *testing.T) {
	enc := NewChaCha20Encryptor()
	nonce0 := enc.GenerateNonce(0)
	nonce1 := enc.GenerateNonce(1)

	if bytes.Equal(nonce0, nonce1) {
		t.Errorf("expected distinct nonces for different chunk indices within same session, got identical: %x", nonce0)
	}
}

func TestChaCha20Encryptor_TagVerificationFailure(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	originalData := []byte("confidential payload data")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: uint32(len(originalData)),
			Index:     7,
		},
		Data: append([]byte(nil), originalData...),
	}

	if err := enc.EncryptChunk(key, chunk); err != nil {
		t.Fatalf("EncryptChunk failed: %v", err)
	}

	// Test case 1: Alter Poly1305 authentication tag (last 16 bytes of ciphertext)
	corruptedTagChunk := *chunk
	corruptedTagChunk.Data = append([]byte(nil), chunk.Data...)
	corruptedTagChunk.Data[len(corruptedTagChunk.Data)-1] ^= 0x01

	if err := enc.DecryptChunk(key, &corruptedTagChunk); err == nil {
		t.Errorf("expected DecryptChunk to fail on altered Poly1305 tag, but succeeded")
	}

	// Test case 2: Alter Header Nonce
	corruptedNonceChunk := *chunk
	corruptedNonceChunk.Data = append([]byte(nil), chunk.Data...)
	corruptedNonceChunk.Header.Nonce[0] ^= 0x01

	if err := enc.DecryptChunk(key, &corruptedNonceChunk); err == nil {
		t.Errorf("expected DecryptChunk to fail on altered nonce, but succeeded")
	}
}
