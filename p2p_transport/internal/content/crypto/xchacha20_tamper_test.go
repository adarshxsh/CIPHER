package crypto

import (
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
)

func TestXChaCha20Encryptor_TamperedHeader(t *testing.T) {
	enc := NewXChaCha20Encryptor()
	key := make([]byte, 32)
	rand.Read(key)

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:   1,
			Index:     5,
			Offset:    1024,
			PlainSize: 5,
		},
		Data: []byte("hello"),
	}

	err := enc.EncryptChunk(key, chunk)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Tamper with the index
	chunk.Header.Index = 6

	err = enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail due to tampered header (index changed)")
	}

	// Fix index, break offset
	chunk.Header.Index = 5
	chunk.Header.Offset = 2048

	err = enc.DecryptChunk(key, chunk)
	if err == nil {
		t.Errorf("expected decryption to fail due to tampered header (offset changed)")
	}

	// Fix offset, verify original decrypts
	chunk.Header.Offset = 1024
	err = enc.DecryptChunk(key, chunk)
	if err != nil {
		t.Errorf("expected decryption to succeed with fixed header, got: %v", err)
	}
}
