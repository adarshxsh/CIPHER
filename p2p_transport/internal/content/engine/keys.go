package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	mu      sync.RWMutex
	baseDir string
}

func NewFSKeyProvider(baseDir string) *FSKeyProvider {
	return &FSKeyProvider{
		baseDir: baseDir,
	}
}

func (p *FSKeyProvider) keyPath(id core.ContentID) string {
	hexID := hex.EncodeToString(id[:])
	return filepath.Join(p.baseDir, "keys", hexID+".key")
}

func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	path := p.keyPath(id)
	key, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("key not found")
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

	keysDir := filepath.Join(p.baseDir, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys directory: %w", err)
	}
	_ = os.Chmod(keysDir, 0700)

	targetPath := p.keyPath(id)
	tmpPath := fmt.Sprintf("%s.key-tmp-%d-%d", targetPath, os.Getpid(), time.Now().UnixNano())

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp key file: %w", err)
	}

	if _, err := f.Write(key); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write key bytes: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync key file: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp key file: %w", err)
	}

	if err := os.Chmod(tmpPath, 0600); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to set permissions on temp key file: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp key file: %w", err)
	}

	_ = os.Chmod(targetPath, 0600)
	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	targetPath := p.keyPath(id)
	info, err := os.Stat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Overwrite key bytes with zeros prior to removal
	if f, err := os.OpenFile(targetPath, os.O_WRONLY, 0600); err == nil {
		zeros := make([]byte, info.Size())
		_, _ = f.Write(zeros)
		_ = f.Sync()
		_ = f.Close()
	}

	return os.Remove(targetPath)
}

