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

// LocalKeyProvider is an implementation of core.KeyProvider.
// If baseDir is provided, keys are automatically persisted to disk under <baseDir>/keys/<ContentID>.key with 0600 permissions.
type LocalKeyProvider struct {
	mu      sync.RWMutex
	keys    map[core.ContentID][]byte
	baseDir string
}

func NewLocalKeyProvider(baseDir ...string) *LocalKeyProvider {
	dir := ""
	if len(baseDir) > 0 {
		dir = baseDir[0]
	}
	return &LocalKeyProvider{
		keys:    make(map[core.ContentID][]byte),
		baseDir: dir,
	}
}

func (p *LocalKeyProvider) SetBaseDir(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.baseDir = dir
}

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	key, exists := p.keys[id]
	if exists {
		p.mu.RUnlock()
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}
	dir := p.baseDir
	p.mu.RUnlock()

	if dir != "" {
		keyPath := filepath.Join(dir, "keys", fmt.Sprintf("%x.key", id))
		if data, err := LoadKeyFromFile(keyPath); err == nil {
			p.mu.Lock()
			p.keys[id] = data
			p.mu.Unlock()
			keyCopy := make([]byte, len(data))
			copy(keyCopy, data)
			return keyCopy, nil
		}
	}

	return nil, errors.New("key not found")
}

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy

	if p.baseDir != "" {
		keyDir := filepath.Join(p.baseDir, "keys")
		if err := os.MkdirAll(keyDir, 0700); err != nil {
			return fmt.Errorf("failed to create key store dir: %w", err)
		}
		keyPath := filepath.Join(keyDir, fmt.Sprintf("%x.key", id))
		if err := os.WriteFile(keyPath, keyCopy, 0600); err != nil {
			return fmt.Errorf("failed to save key file: %w", err)
		}
		_ = os.Chmod(keyPath, 0600)
	}

	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.keys, id)

	if p.baseDir != "" {
		keyPath := filepath.Join(p.baseDir, "keys", fmt.Sprintf("%x.key", id))
		_ = os.Remove(keyPath)
	}
	return nil
}

// LoadKeyFromFile reads key material from a path on disk.
// Accepts raw binary key (32 bytes) or hex string (64 characters).
func LoadKeyFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}
	if len(data) == 32 {
		return data, nil
	}
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) == 64 {
		kBytes, err := hex.DecodeString(trimmed)
		if err == nil && len(kBytes) == 32 {
			return kBytes, nil
		}
	}
	if kBytes, err := hex.DecodeString(trimmed); err == nil && len(kBytes) == 32 {
		return kBytes, nil
	}
	return nil, fmt.Errorf("invalid key file format in %s (expected 32 raw bytes or 64 hex characters)", path)
}
