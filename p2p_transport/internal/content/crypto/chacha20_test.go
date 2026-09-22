package crypto

import (
	"bytes"
	"crypto/rand"
	"fmt"
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

func TestChaCha20Encryptor_RandomNoncesDistinct(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	const count = 10000
	seenNonces := make(map[[24]byte]bool, count)

	for i := 0; i < count; i++ {
		chunk := &core.Chunk{
			Header: core.ChunkHeader{
				Index:     uint32(i),
				PlainSize: 10,
			},
			Data: []byte("chunk-data"),
		}

		if err := enc.EncryptChunk(key, chunk); err != nil {
			t.Fatalf("failed to encrypt chunk %d: %v", i, err)
		}

		if seenNonces[chunk.Header.Nonce] {
			t.Fatalf("duplicate nonce detected at iteration %d: %x", i, chunk.Header.Nonce)
		}
		seenNonces[chunk.Header.Nonce] = true
	}

	if len(seenNonces) != count {
		t.Errorf("expected %d unique nonces, got %d", count, len(seenNonces))
	}
}

func TestChaCha20Encryptor_OutOfOrderDecryption(t *testing.T) {
	enc := NewChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	chunks := make([]*core.Chunk, 5)
	originalData := make([][]byte, 5)

	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("out-of-order-chunk-%d", i))
		originalData[i] = data
		chunks[i] = &core.Chunk{
			Header: core.ChunkHeader{
				Index:     uint32(i),
				PlainSize: uint32(len(data)),
			},
			Data: append([]byte(nil), data...),
		}
		if err := enc.EncryptChunk(key, chunks[i]); err != nil {
			t.Fatalf("failed to encrypt chunk %d: %v", i, err)
		}
	}

	// Decrypt in non-sequential order (e.g., indices 3, 0, 4, 1, 2)
	order := []int{3, 0, 4, 1, 2}
	for _, idx := range order {
		chunk := chunks[idx]
		if err := enc.DecryptChunk(key, chunk); err != nil {
			t.Fatalf("failed to decrypt chunk index %d: %v", idx, err)
		}
		if !bytes.Equal(chunk.Data, originalData[idx]) {
			t.Errorf("chunk index %d decrypted data mismatch: got %s, want %s", idx, chunk.Data, originalData[idx])
		}
	}
}
