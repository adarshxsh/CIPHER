package crypto

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// ChaCha20Encryptor implements core.Encryptor using standard ChaCha20-Poly1305.
// It uses a deterministic 12-byte nonce derived from the chunk index:
// nonce = first_12_bytes(SHA-256("cipher-nonce" || uint64(index)))
type ChaCha20Encryptor struct{}

func NewChaCha20Encryptor() *ChaCha20Encryptor {
	return &ChaCha20Encryptor{}
}

func (e *ChaCha20Encryptor) generateNonce(index uint32) []byte {
	var buf [20]byte
	copy(buf[0:12], "cipher-nonce")
	binary.LittleEndian.PutUint64(buf[12:20], uint64(index))
	sum := sha256.Sum256(buf[:])
	nonce := make([]byte, 12)
	copy(nonce, sum[:12])
	return nonce
}

func (e *ChaCha20Encryptor) EncryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	nonce := e.generateNonce(chunk.Header.Index)
	ciphertext := aead.Seal(chunk.Data[:0], nonce, chunk.Data, nil)

	copy(chunk.Header.Nonce[:], nonce)
	chunk.Header.CipherSize = uint32(len(ciphertext))
	chunk.Data = ciphertext

	return nil
}

func (e *ChaCha20Encryptor) DecryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	if chunk.Header.CipherSize != uint32(len(chunk.Data)) {
		return errors.New("cipher size mismatch in header")
	}

	plaintext, err := aead.Open(chunk.Data[:0], chunk.Header.Nonce[:], chunk.Data, nil)
	if err != nil {
		return fmt.Errorf("failed to decrypt chunk: %w", err)
	}

	if chunk.Header.PlainSize != uint32(len(plaintext)) {
		return errors.New("plain size mismatch in header")
	}

	chunk.Data = plaintext
	return nil
}
