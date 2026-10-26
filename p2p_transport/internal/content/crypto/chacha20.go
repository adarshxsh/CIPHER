package crypto

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"cipher/internal/content/core"
	pool "github.com/libp2p/go-buffer-pool"
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
	h := sha256.New()
	h.Write([]byte("cipher-nonce"))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(index))
	h.Write(b)
	sum := h.Sum(nil)
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
	outLen := len(chunk.Data) + aead.Overhead()
	outBuf := pool.Get(outLen)
	ciphertext := aead.Seal(outBuf[:0], nonce, chunk.Data, nil)

	oldData := chunk.Data
	copy(chunk.Header.Nonce[:], nonce)
	chunk.Header.CipherSize = uint32(len(ciphertext))
	chunk.Data = ciphertext

	if len(oldData) > 0 {
		pool.Put(oldData)
	}

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

	outLen := int(chunk.Header.PlainSize)
	outBuf := pool.Get(outLen)
	plaintext, err := aead.Open(outBuf[:0], chunk.Header.Nonce[:], chunk.Data, nil)
	if err != nil {
		pool.Put(outBuf)
		return fmt.Errorf("failed to decrypt chunk: %w", err)
	}

	if chunk.Header.PlainSize != uint32(len(plaintext)) {
		pool.Put(outBuf)
		return errors.New("plain size mismatch in header")
	}

	oldData := chunk.Data
	chunk.Data = plaintext

	if len(oldData) > 0 {
		pool.Put(oldData)
	}

	return nil
}
