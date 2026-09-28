package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cipher/internal/content/core"
)

// FSKeyProvider implements core.KeyProvider with persistent storage on disk
// and an in-memory cache for fast lookups.
type FSKeyProvider struct {
	mu       sync.RWMutex
	storeDir string
	keysDir  string
	keys     map[core.ContentID][]byte
}

// NewFSKeyProvider creates or initializes an FSKeyProvider bound to storeDir.
// Key files will be persisted under <storeDir>/keys/ with 0600 file permissions.
func NewFSKeyProvider(storeDir string) (*FSKeyProvider, error) {
	keysDir := storeDir
	if filepath.Base(storeDir) != "keys" {
		keysDir = filepath.Join(storeDir, "keys")
	}

	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create keys directory: %w", err)
	}

	p := &FSKeyProvider{
		storeDir: storeDir,
		keysDir:  keysDir,
		keys:     make(map[core.ContentID][]byte),
	}

	return p, nil
}

// MustNewFSKeyProvider creates an FSKeyProvider or panics if initialization fails.
func MustNewFSKeyProvider(storeDir string) *FSKeyProvider {
	p, err := NewFSKeyProvider(storeDir)
	if err != nil {
		panic(err)
	}
	return p
}

func (p *FSKeyProvider) keyFilePath(id core.ContentID) string {
	return filepath.Join(p.keysDir, fmt.Sprintf("%x.key", id))
}

// Get retrieves a key for contentID, checking in-memory cache first, then disk.
func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	key, exists := p.keys[id]
	if exists {
		p.mu.RUnlock()
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}
	p.mu.RUnlock()

	// Try reading from disk
	keyPath := p.keyFilePath(id)
	data, err := os.ReadFile(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("key not found")
		}
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	// Support both 32-byte raw binary keys and 64-character hex strings
	var keyBytes []byte
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) == 64 {
		decoded, err := hex.DecodeString(trimmed)
		if err != nil {
			return nil, fmt.Errorf("corrupted hex key in file %s: %w", keyPath, err)
		}
		keyBytes = decoded
	} else if len(data) == 32 {
		keyBytes = data
	} else {
		return nil, fmt.Errorf("invalid key length in file %s", keyPath)
	}

	// Update cache
	p.mu.Lock()
	keyCopy := make([]byte, len(keyBytes))
	copy(keyCopy, keyBytes)
	p.keys[id] = keyCopy
	p.mu.Unlock()

	return keyCopy, nil
}

// Put writes key atomically to disk at <storeDir>/keys/<ContentID>.key with 0600 permissions and updates cache.
func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	if len(key) == 0 {
		return errors.New("cannot store empty key")
	}

	if err := os.MkdirAll(p.keysDir, 0700); err != nil {
		return fmt.Errorf("failed to ensure keys dir: %w", err)
	}

	// Write atomically using temporary file
	tmpFile, err := os.CreateTemp(p.keysDir, ".key-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary key file: %w", err)
	}
	tmpName := tmpFile.Name()

	// Ensure temporary file is cleaned up if rename fails
	success := false
	defer func() {
		if !success {
			tmpFile.Close()
			os.Remove(tmpName)
		}
	}()

	if _, err := tmpFile.Write(key); err != nil {
		return fmt.Errorf("failed to write key to temporary file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync key file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary key file: %w", err)
	}

	if err := os.Chmod(tmpName, 0600); err != nil {
		return fmt.Errorf("failed to set 0600 permissions on key file: %w", err)
	}

	keyPath := p.keyFilePath(id)
	if err := os.Rename(tmpName, keyPath); err != nil {
		return fmt.Errorf("failed to rename key file: %w", err)
	}

	// Ensure target file permissions are 0600
	_ = os.Chmod(keyPath, 0600)

	success = true

	// Update in-memory cache
	p.mu.Lock()
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	p.mu.Unlock()

	return nil
}

// Delete removes key from cache and disk.
func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	delete(p.keys, id)
	p.mu.Unlock()

	keyPath := p.keyFilePath(id)
	err := os.Remove(keyPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}

	return nil
}
