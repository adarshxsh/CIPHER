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
		return nil, errors.New("key not found")
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

// FSKeyProvider is a persistent filesystem-backed implementation of core.KeyProvider.
type FSKeyProvider struct {
	storePath string
	mu        sync.RWMutex
	cache     map[core.ContentID][]byte
}

func NewFSKeyProvider(storePath string) *FSKeyProvider {
	return &FSKeyProvider{
		storePath: storePath,
		cache:     make(map[core.ContentID][]byte),
	}
}

func (p *FSKeyProvider) keyPath(id core.ContentID) string {
	hexID := hex.EncodeToString(id[:])
	return filepath.Join(p.storePath, "keys", hexID+".key")
}

func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	if key, exists := p.cache[id]; exists {
		p.mu.RUnlock()
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check cache after acquiring write lock
	if key, exists := p.cache[id]; exists {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}

	keyFilePath := p.keyPath(id)
	keyData, err := os.ReadFile(keyFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("key not found")
		}
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	keyCopy := make([]byte, len(keyData))
	copy(keyCopy, keyData)
	p.cache[id] = keyCopy

	result := make([]byte, len(keyData))
	copy(result, keyData)
	return result, nil
}

func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	keysDir := filepath.Join(p.storePath, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys directory: %w", err)
	}
	if err := os.Chmod(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to set permissions on keys directory: %w", err)
	}

	keyFilePath := p.keyPath(id)
	if err := os.WriteFile(keyFilePath, key, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	if err := os.Chmod(keyFilePath, 0600); err != nil {
		return fmt.Errorf("failed to set permissions on key file: %w", err)
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.cache[id] = keyCopy

	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.cache, id)

	keyFilePath := p.keyPath(id)
	if err := os.Remove(keyFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}

	return nil
}

