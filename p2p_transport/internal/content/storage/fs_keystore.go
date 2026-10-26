package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"cipher/internal/content/core"
)

// FSKeyStore implements core.KeyProvider using local filesystem storage.
// Key directory is created with 0700 permissions and key files with 0600 permissions.
type FSKeyStore struct {
	mu      sync.RWMutex
	keysDir string
}

// NewFSKeyStore creates or initializes a new FSKeyStore at the target base directory.
// The key files will be stored in <baseDir>/keys (or baseDir if it already ends in "keys").
func NewFSKeyStore(baseDir string) (*FSKeyStore, error) {
	keysDir := baseDir
	if filepath.Base(baseDir) != "keys" {
		keysDir = filepath.Join(baseDir, "keys")
	}

	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create keys directory: %w", err)
	}
	if err := os.Chmod(keysDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to enforce 0700 permissions on keys directory: %w", err)
	}

	return &FSKeyStore{
		keysDir: keysDir,
	}, nil
}

// Get retrieves a key by ContentID from disk storage.
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
				keyCopy := make([]byte, len(altData))
				copy(keyCopy, altData)
				return keyCopy, nil
			}
			return nil, errors.New("key not found")
		}
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	keyCopy := make([]byte, len(data))
	copy(keyCopy, data)
	return keyCopy, nil
}

// Put saves a key by ContentID to disk storage with 0600 POSIX permissions.
func (s *FSKeyStore) Put(ctx context.Context, id core.ContentID, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys directory: %w", err)
	}
	if err := os.Chmod(s.keysDir, 0700); err != nil {
		return fmt.Errorf("failed to enforce 0700 permissions on keys directory: %w", err)
	}

	filename := hex.EncodeToString(id[:]) + ".key"
	path := filepath.Join(s.keysDir, filename)

	if err := os.WriteFile(path, key, 0600); err != nil {
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
