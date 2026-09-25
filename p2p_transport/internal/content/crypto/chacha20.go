package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// ChaCha20Encryptor implements core.Encryptor using standard ChaCha20-Poly1305.
// It uses a 12-byte CSPRNG random nonce generated via crypto/rand per chunk.
type ChaCha20Encryptor struct{}

func NewChaCha20Encryptor() *ChaCha20Encryptor {
	return &ChaCha20Encryptor{}
}

func (e *ChaCha20Encryptor) EncryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	if _, err := rand.Read(chunk.Header.Nonce[:]); err != nil {
		return fmt.Errorf("failed to generate random nonce: %w", err)
	}

	ciphertext := aead.Seal(nil, chunk.Header.Nonce[:], chunk.Data, nil)

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

	plaintext, err := aead.Open(nil, chunk.Header.Nonce[:], chunk.Data, nil)
	if err != nil {
		return fmt.Errorf("failed to decrypt chunk: %w", err)
	}

	if chunk.Header.PlainSize != uint32(len(plaintext)) {
		return errors.New("plain size mismatch in header")
	}

	chunk.Data = plaintext
	return nil
}

