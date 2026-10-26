package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// ChaCha20Encryptor implements core.Encryptor using XChaCha20-Poly1305.
// It uses a 24-byte CSPRNG random nonce generated for every chunk encryption pass,
// which is prepended to the ciphertext payload frame.
type ChaCha20Encryptor struct{}

func NewChaCha20Encryptor() *ChaCha20Encryptor {
	return &ChaCha20Encryptor{}
}

func (e *ChaCha20Encryptor) EncryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("failed to generate random nonce: %w", err)
	}

	ciphertext := aead.Seal(nonce, nonce, chunk.Data, nil)

	copy(chunk.Header.Nonce[:], nonce)
	chunk.Header.CipherSize = uint32(len(ciphertext))
	chunk.Data = ciphertext

	return nil
}

func (e *ChaCha20Encryptor) DecryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	if chunk.Header.CipherSize != uint32(len(chunk.Data)) {
		return errors.New("cipher size mismatch in header")
	}

	if len(chunk.Data) < chacha20poly1305.NonceSizeX+chacha20poly1305.Overhead {
		return errors.New("ciphertext payload frame too short")
	}

	nonce := chunk.Data[:chacha20poly1305.NonceSizeX]
	ciphertextPayload := chunk.Data[chacha20poly1305.NonceSizeX:]

	plaintext, err := aead.Open(nil, nonce, ciphertextPayload, nil)
	if err != nil {
		return fmt.Errorf("failed to decrypt chunk: %w", err)
	}

	if chunk.Header.PlainSize != uint32(len(plaintext)) {
		return errors.New("plain size mismatch in header")
	}

	copy(chunk.Header.Nonce[:], nonce)
	chunk.Data = plaintext
	return nil
}
