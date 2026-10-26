package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// ChaCha20Encryptor implements core.Encryptor using standard ChaCha20-Poly1305.
// It uses cryptographically random 12-byte nonces generated via crypto/rand and embedded in chunk headers.
type ChaCha20Encryptor struct{}

func NewChaCha20Encryptor() *ChaCha20Encryptor {
	return &ChaCha20Encryptor{}
}

func (e *ChaCha20Encryptor) generateNonce() ([12]byte, error) {
	var nonce [12]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nonce, fmt.Errorf("failed to generate random nonce: %w", err)
	}
	return nonce, nil
}

func (e *ChaCha20Encryptor) EncryptChunk(key []byte, chunk *core.Chunk) error {
	if chunk == nil {
		return errors.New("chunk cannot be nil")
	}

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	nonce, err := e.generateNonce()
	if err != nil {
		return err
	}

	ciphertext := aead.Seal(nil, nonce[:], chunk.Data, nil)

	chunk.Header.Nonce = nonce
	chunk.Header.CipherSize = uint32(len(ciphertext))
	chunk.Data = ciphertext

	return nil
}

func (e *ChaCha20Encryptor) DecryptChunk(key []byte, chunk *core.Chunk) error {
	if chunk == nil {
		return errors.New("chunk cannot be nil")
	}

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
