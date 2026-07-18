package crypto

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// XChaCha20Encryptor implements core.Encryptor using XChaCha20-Poly1305.
type XChaCha20Encryptor struct{}

func NewXChaCha20Encryptor() *XChaCha20Encryptor {
	return &XChaCha20Encryptor{}
}

func deriveChunkKey(masterKey []byte, index uint32) ([]byte, error) {
	info := make([]byte, 4)
	binary.LittleEndian.PutUint32(info, index)

	hkdf := hkdf.New(sha256.New, masterKey, nil, info)
	chunkKey := make([]byte, 32)
	if _, err := io.ReadFull(hkdf, chunkKey); err != nil {
		return nil, fmt.Errorf("hkdf derivation failed: %w", err)
	}
	return chunkKey, nil
}

func (e *XChaCha20Encryptor) EncryptChunk(masterKey []byte, chunk *core.Chunk) error {
	chunkKey, err := deriveChunkKey(masterKey, chunk.Header.Index)
	if err != nil {
		return err
	}

	aead, err := chacha20poly1305.NewX(chunkKey)
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

func (e *XChaCha20Encryptor) DecryptChunk(masterKey []byte, chunk *core.Chunk) error {
	chunkKey, err := deriveChunkKey(masterKey, chunk.Header.Index)
	if err != nil {
		return err
	}

	aead, err := chacha20poly1305.NewX(chunkKey)
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
