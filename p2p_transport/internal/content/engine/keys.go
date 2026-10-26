package engine

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

// ErrKeyNotFound indicates a requested content decryption key was not found.
var ErrKeyNotFound = errors.New("key not found")

// FSKeyProvider implements core.KeyProvider by persisting content decryption keys
// to disk under <storePath>/keys/<hex(ContentID)>.key with 0600 file permissions
// and 0700 directory permissions.
type FSKeyProvider struct {
	baseDir string
	mu      sync.RWMutex
}

func NewFSKeyProvider(storePath string) *FSKeyProvider {
	if storePath == "" {
		storePath = "."
	}
	keysDir := filepath.Join(storePath, "keys")
	_ = os.MkdirAll(keysDir, 0700)
	_ = os.Chmod(keysDir, 0700)
	return &FSKeyProvider{
		baseDir: storePath,
	}
}

func (p *FSKeyProvider) keyDir() string {
	return filepath.Join(p.baseDir, "keys")
}

func (p *FSKeyProvider) keyPath(id core.ContentID) string {
	encoded := hex.EncodeToString(id[:])
	return filepath.Join(p.keyDir(), fmt.Sprintf("%s.key", encoded))
}

func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	keyPath := p.keyPath(id)
	key, err := os.ReadFile(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	keysDir := p.keyDir()
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys directory: %w", err)
	}
	if err := os.Chmod(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to set keys directory permissions: %w", err)
	}

	keyPath := p.keyPath(id)
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	if err := os.WriteFile(keyPath, keyCopy, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	if err := os.Chmod(keyPath, 0600); err != nil {
		return fmt.Errorf("failed to set key file permissions: %w", err)
	}

	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	keyPath := p.keyPath(id)
	if err := os.Remove(keyPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}
	return nil
}

// LocalKeyProvider is an in-memory implementation of core.KeyProvider.
type LocalKeyProvider struct {
	mu   sync.RWMutex
	keys map[core.ContentID][]byte
}

func NewLocalKeyProvider() *LocalKeyProvider {
	return &LocalKeyProvider{
		keys: make(map[core.ContentID][]byte),
	}
}

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	key, exists := p.keys[id]
	if !exists {
		return nil, ErrKeyNotFound
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.keys, id)
	return nil
}
