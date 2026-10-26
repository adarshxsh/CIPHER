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
	rand.Read(key)

	data := []byte("identical payload for both chunks")

	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{PlainSize: uint32(len(data))},
		Data:   append([]byte(nil), data...),
	}
	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{PlainSize: uint32(len(data))},
		Data:   append([]byte(nil), data...),
	}

	if err := enc.EncryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to encrypt chunk1: %v", err)
	}
	if err := enc.EncryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to encrypt chunk2: %v", err)
	}

	if bytes.Equal(chunk1.Header.Nonce[:], chunk2.Header.Nonce[:]) {
		t.Errorf("expected nonces to be random and distinct, but got identical nonces")
	}

	if bytes.Equal(chunk1.Data, chunk2.Data) {
		t.Errorf("expected ciphertexts to differ for distinct random nonces")
	}

	if err := enc.DecryptChunk(key, chunk1); err != nil {
		t.Fatalf("failed to decrypt chunk1: %v", err)
	}
	if err := enc.DecryptChunk(key, chunk2); err != nil {
		t.Fatalf("failed to decrypt chunk2: %v", err)
	}

	if !bytes.Equal(chunk1.Data, data) || !bytes.Equal(chunk2.Data, data) {
		t.Errorf("decrypted data mismatch")
	}
}

func TestChaCha20Encryptor_ErrorHandling(t *testing.T) {
	enc := NewChaCha20Encryptor()
	validKey := make([]byte, 32)
	rand.Read(validKey)
	invalidKey := make([]byte, 16) // invalid size for ChaCha20-Poly1305

	t.Run("nil chunk", func(t *testing.T) {
		if err := enc.EncryptChunk(validKey, nil); err == nil {
			t.Errorf("expected error when encrypting nil chunk")
		}
		if err := enc.DecryptChunk(validKey, nil); err == nil {
			t.Errorf("expected error when decrypting nil chunk")
		}
	})

	t.Run("invalid key length", func(t *testing.T) {
		chunk := &core.Chunk{
			Header: core.ChunkHeader{PlainSize: 4},
			Data:   []byte("test"),
		}
		if err := enc.EncryptChunk(invalidKey, chunk); err == nil {
			t.Errorf("expected error for invalid key in EncryptChunk")
		}
		if err := enc.DecryptChunk(invalidKey, chunk); err == nil {
			t.Errorf("expected error for invalid key in DecryptChunk")
		}
	})

	t.Run("cipher size mismatch", func(t *testing.T) {
		chunk := &core.Chunk{
			Header: core.ChunkHeader{PlainSize: 4},
			Data:   []byte("test"),
		}
		if err := enc.EncryptChunk(validKey, chunk); err != nil {
			t.Fatalf("failed to encrypt: %v", err)
		}
		chunk.Header.CipherSize += 10
		if err := enc.DecryptChunk(validKey, chunk); err == nil {
			t.Errorf("expected error on CipherSize mismatch")
		}
	})

	t.Run("plain size mismatch", func(t *testing.T) {
		chunk := &core.Chunk{
			Header: core.ChunkHeader{PlainSize: 4},
			Data:   []byte("test"),
		}
		if err := enc.EncryptChunk(validKey, chunk); err != nil {
			t.Fatalf("failed to encrypt: %v", err)
		}
		chunk.Header.PlainSize += 10
		if err := enc.DecryptChunk(validKey, chunk); err == nil {
			t.Errorf("expected error on PlainSize mismatch")
		}
	})
}
