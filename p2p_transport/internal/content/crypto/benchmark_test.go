package crypto_test

import (
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
)

func BenchmarkChaCha20Encryptor(b *testing.B) {
	enc := crypto.NewChaCha20Encryptor()
	
	key, err := core.NewRandomSecretKey(32)
	if err != nil {
		b.Fatalf("failed to create key: %v", err)
	}
	defer key.Destroy()

	chunkSize := 256 * 1024
	data := make([]byte, chunkSize)
	rand.Read(data)

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			PlainSize: uint32(chunkSize),
		},
		Data: data,
	}

	b.SetBytes(int64(chunkSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Encrypt in place
		err := enc.EncryptChunk(key, chunk)
		if err != nil {
			b.Fatalf("Encrypt failed: %v", err)
		}
		// Reset for next iteration (doesn't have to be plaintext, just needs to encrypt again)
		chunk.Data = chunk.Data[:chunkSize]
	}
}
