package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// FSKeyStore implements core.KeyProvider using local filesystem storage.
// Keys are encrypted at rest using ChaCha20-Poly1305 with a local master key.
// Directory permissions are 0700 and key files are 0600.
type FSKeyStore struct {
	mu        sync.RWMutex
	keysDir   string
	masterKey []byte
}

// NewFSKeyStore creates or initializes a new FSKeyStore at baseDir.
// If baseDir is empty, it checks CIPHER_CONFIG_DIR or defaults to user config dir (~/.config/cipher).
func NewFSKeyStore(baseDir string) (*FSKeyStore, error) {
	keysDir := baseDir
	if keysDir == "" {
		configDir := os.Getenv("CIPHER_CONFIG_DIR")
		if configDir == "" {
			var err error
			configDir, err = os.UserConfigDir()
			if err != nil {
				return nil, fmt.Errorf("failed to get user config dir: %w", err)
			}
			configDir = filepath.Join(configDir, "cipher")
		}
		keysDir = configDir
	}

	if filepath.Base(keysDir) != "keys" {
		keysDir = filepath.Join(keysDir, "keys")
	}

	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create keys directory: %w", err)
	}
	if err := os.Chmod(keysDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to enforce 0700 permissions on keys directory: %w", err)
	}

	// Load or generate master key for keystore encryption
	masterKeyPath := filepath.Join(keysDir, ".masterkey")
	var masterKey []byte
	if data, err := os.ReadFile(masterKeyPath); err == nil && len(data) == chacha20poly1305.KeySize {
		masterKey = data
	} else {
		masterKey = make([]byte, chacha20poly1305.KeySize)
		if _, err := io.ReadFull(rand.Reader, masterKey); err != nil {
			return nil, fmt.Errorf("failed to generate master key: %w", err)
		}
		if err := os.WriteFile(masterKeyPath, masterKey, 0600); err != nil {
			return nil, fmt.Errorf("failed to write master key: %w", err)
		}
		_ = os.Chmod(masterKeyPath, 0600)
	}

	return &FSKeyStore{
		keysDir:   keysDir,
		masterKey: masterKey,
	}, nil
}

// Get retrieves and decrypts a key by ContentID from disk storage.
func (s *FSKeyStore) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filename := hex.EncodeToString(id[:]) + ".key"
	path := filepath.Join(s.keysDir, filename)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Fallback check without .key extension
			altPath := filepath.Join(s.keysDir, hex.EncodeToString(id[:]))
			altData, altErr := os.ReadFile(altPath)
			if altErr == nil {
				data = altData
			} else {
				return nil, errors.New("key not found")
			}
		} else {
			return nil, fmt.Errorf("failed to read key file: %w", err)
		}
	}

	// Decrypt stored key using ChaCha20-Poly1305
	aead, err := chacha20poly1305.New(s.masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	nonceSize := aead.NonceSize()
	if len(data) > nonceSize {
		nonce, ciphertext := data[:nonceSize], data[nonceSize:]
		plainKey, err := aead.Open(nil, nonce, ciphertext, id[:])
		if err == nil {
			return plainKey, nil
		}
		// Attempt decryption without additional data if id[:] AD fails
		plainKey, err = aead.Open(nil, nonce, ciphertext, nil)
		if err == nil {
			return plainKey, nil
		}
	}

	// Fallback for unencrypted 32-byte raw key file
	if len(data) == 32 {
		keyCopy := make([]byte, 32)
		copy(keyCopy, data)
		return keyCopy, nil
	}

	return nil, errors.New("failed to decrypt key file")
}

// Put encrypts and saves a key by ContentID to disk storage with 0600 POSIX permissions.
func (s *FSKeyStore) Put(ctx context.Context, id core.ContentID, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys directory: %w", err)
	}
	_ = os.Chmod(s.keysDir, 0700)

	aead, err := chacha20poly1305.New(s.masterKey)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := aead.Seal(nonce, nonce, key, id[:])

	filename := hex.EncodeToString(id[:]) + ".key"
	path := filepath.Join(s.keysDir, filename)

	if err := os.WriteFile(path, ciphertext, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		return fmt.Errorf("failed to enforce 0600 permissions on key file: %w", err)
	}

	return nil
}

// Delete removes a key by ContentID from disk storage.
func (s *FSKeyStore) Delete(ctx context.Context, id core.ContentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filename := hex.EncodeToString(id[:]) + ".key"
	path := filepath.Join(s.keysDir, filename)

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}

	altPath := filepath.Join(s.keysDir, hex.EncodeToString(id[:]))
	_ = os.Remove(altPath)

	return nil
}

// Ensure FSKeyStore implements cipher.AEAD / core.KeyProvider
var _ core.KeyProvider = (*FSKeyStore)(nil)
