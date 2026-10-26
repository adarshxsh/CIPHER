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

// SessionIVSize defines the size of the session IV in bytes (12 bytes for ChaCha20-Poly1305).
const SessionIVSize = 12

// ChaCha20Encryptor implements core.Encryptor using standard ChaCha20-Poly1305.
// It mixes a cryptographically random 12-byte session IV with the chunk index
// via SHA-256 HKDF to ensure unique nonces across transfer sessions.
type ChaCha20Encryptor struct {
	sessionIV []byte
}

// NewChaCha20Encryptor initializes a ChaCha20Encryptor with a fresh random 12-byte session IV.
func NewChaCha20Encryptor() *ChaCha20Encryptor {
	iv := make([]byte, SessionIVSize)
	if _, err := rand.Read(iv); err != nil {
		panic(fmt.Sprintf("failed to generate random session IV: %v", err))
	}
	return &ChaCha20Encryptor{
		sessionIV: iv,
	}
}

// NewChaCha20EncryptorWithIV initializes a ChaCha20Encryptor with a specific 12-byte session IV.
func NewChaCha20EncryptorWithIV(sessionIV []byte) *ChaCha20Encryptor {
	iv := make([]byte, SessionIVSize)
	copy(iv, sessionIV)
	return &ChaCha20Encryptor{
		sessionIV: iv,
	}
}

// SessionIV returns a copy of the encryptor's current session IV.
func (e *ChaCha20Encryptor) SessionIV() []byte {
	iv := make([]byte, len(e.sessionIV))
	copy(iv, e.sessionIV)
	return iv
}

// SetSessionIV sets a new session IV for the encryptor.
func (e *ChaCha20Encryptor) SetSessionIV(iv []byte) {
	e.sessionIV = make([]byte, SessionIVSize)
	copy(e.sessionIV, iv)
}

// ResetSessionIV generates and sets a new random 12-byte session IV.
func (e *ChaCha20Encryptor) ResetSessionIV() error {
	iv := make([]byte, SessionIVSize)
	if _, err := rand.Read(iv); err != nil {
		return fmt.Errorf("failed to generate random session IV: %w", err)
	}
	e.sessionIV = iv
	return nil
}

// GenerateNonce mixes a 12-byte session IV with a chunk index using SHA-256 HKDF
// to produce a unique 12-byte ChaCha20 nonce.
func GenerateNonce(sessionIV []byte, index uint32) []byte {
	indexBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(indexBytes, uint64(index))

	// HKDF-SHA256: secret = sessionIV, salt = "cipher-nonce", info = uint64(index)
	kdf := hkdf.New(sha256.New, sessionIV, []byte("cipher-nonce"), indexBytes)
	nonce := make([]byte, 12)
	_, _ = io.ReadFull(kdf, nonce)
	return nonce
}

// GenerateNonce derives a 12-byte nonce using the encryptor's session IV and chunk index.
func (e *ChaCha20Encryptor) GenerateNonce(index uint32) []byte {
	return GenerateNonce(e.sessionIV, index)
}

func (e *ChaCha20Encryptor) generateNonce(index uint32) []byte {
	return e.GenerateNonce(index)
}

func (e *ChaCha20Encryptor) EncryptChunk(key []byte, chunk *core.Chunk) error {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	nonce := e.GenerateNonce(chunk.Header.Index)
	ciphertext := aead.Seal(nil, nonce, chunk.Data, nil)

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
