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

var ErrKeyNotFound = errors.New("key not found")

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
	// return a copy to prevent mutation
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

// FSKeyProvider implements core.KeyProvider using local disk storage.
// Keys are stored in <baseDir>/keys/<hex(ContentID)>.key with 0600 file permissions and 0700 directory permissions.
type FSKeyProvider struct {
	baseDir string
}

func NewFSKeyProvider(baseDir string) *FSKeyProvider {
	return &FSKeyProvider{
		baseDir: baseDir,
	}
}

func (p *FSKeyProvider) keyPath(id core.ContentID) string {
	encoded := hex.EncodeToString(id[:])
	return filepath.Join(p.baseDir, "keys", encoded+".key")
}

func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	keyPath := p.keyPath(id)
	key, err := os.ReadFile(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}
	if len(key) == 0 {
		return nil, ErrKeyNotFound
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	keysDir := filepath.Join(p.baseDir, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys dir: %w", err)
	}
	keyPath := p.keyPath(id)
	if err := os.WriteFile(keyPath, key, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	keyPath := p.keyPath(id)
	err := os.Remove(keyPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}
	return nil
}

