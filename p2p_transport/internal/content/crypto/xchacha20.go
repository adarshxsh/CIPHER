package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// XChaCha20Encryptor implements core.Encryptor using XChaCha20-Poly1305
// with cryptographically secure 24-byte random nonces generated via CSPRNG.
type XChaCha20Encryptor struct{}

func NewXChaCha20Encryptor() *XChaCha20Encryptor {
	return &XChaCha20Encryptor{}
}

// ChaCha20Encryptor is an alias for XChaCha20Encryptor for backward compatibility.
type ChaCha20Encryptor = XChaCha20Encryptor

// NewChaCha20Encryptor returns a new XChaCha20Encryptor instance.
func NewChaCha20Encryptor() *XChaCha20Encryptor {
	return NewXChaCha20Encryptor()
}

func (e *XChaCha20Encryptor) EncryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.NewX(key)
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

func (e *XChaCha20Encryptor) DecryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.NewX(key)
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
