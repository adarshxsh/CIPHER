package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// XChaCha20Encryptor implements core.Encryptor using XChaCha20-Poly1305.
type XChaCha20Encryptor struct{}

func NewXChaCha20Encryptor() *XChaCha20Encryptor {
	return &XChaCha20Encryptor{}
}

func (e *XChaCha20Encryptor) EncryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	nonce := make([]byte, aead.NonceSize(), aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("failed to read random nonce: %w", err)
	}

	aad, err := SerializeAAD(&chunk.Header)
	if err != nil {
		return fmt.Errorf("failed to serialize AAD: %w", err)
	}

	ciphertext := aead.Seal(nil, nonce, chunk.Data, aad)

	copy(chunk.Header.Nonce[:], nonce)
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

	aad, err := SerializeAAD(&chunk.Header)
	if err != nil {
		return fmt.Errorf("failed to serialize AAD: %w", err)
	}

	plaintext, err := aead.Open(nil, chunk.Header.Nonce[:], chunk.Data, aad)
	if err != nil {
		return fmt.Errorf("failed to decrypt chunk: %w", err)
	}

	if chunk.Header.PlainSize != uint32(len(plaintext)) {
		return errors.New("plain size mismatch in header")
	}

	chunk.Data = plaintext
	return nil
}
