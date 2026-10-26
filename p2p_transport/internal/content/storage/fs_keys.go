package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"cipher/internal/content/core"
)

// FSKeyProvider implements core.KeyProvider by persisting content encryption keys to local disk storage.
type FSKeyProvider struct {
	baseDir string
	mu      sync.RWMutex
}

var _ core.KeyProvider = (*FSKeyProvider)(nil)

// NewFSKeyProvider creates a new file-backed key provider storing keys under <baseDir>/keys.
func NewFSKeyProvider(baseDir string) *FSKeyProvider {
	return &FSKeyProvider{
		baseDir: baseDir,
	}
}

func (p *FSKeyProvider) keyPath(id core.ContentID) string {
	encoded := hex.EncodeToString(id[:])
	return filepath.Join(p.baseDir, "keys", encoded+".key")
}

// Get retrieves the encryption key for the given content ID from disk.
func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	path := p.keyPath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("key not found: %w", err)
	}

	keyCopy := make([]byte, len(data))
	copy(keyCopy, data)
	return keyCopy, nil
}

// Put writes the encryption key for the given content ID to disk atomically.
func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	keysDir := filepath.Join(p.baseDir, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys dir: %w", err)
	}
	_ = os.Chmod(keysDir, 0700)

	targetPath := p.keyPath(id)
	tmpPath := filepath.Join(keysDir, fmt.Sprintf(".key-tmp-%x-%d", id[:4], os.Getpid()))

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp key file: %w", err)
	}
	_ = os.Chmod(tmpPath, 0600)

	if _, err := f.Write(key); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write key data: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to fsync key file: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close key file: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename key file: %w", err)
	}

	return nil
}

// Delete overwrites and removes the key file corresponding to the given content ID.
func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	targetPath := p.keyPath(id)

	// Zero out key bytes on disk prior to removal if file exists
	if fi, err := os.Stat(targetPath); err == nil {
		if f, err := os.OpenFile(targetPath, os.O_WRONLY, 0600); err == nil {
			zeros := make([]byte, fi.Size())
			_, _ = f.Write(zeros)
			_ = f.Sync()
			_ = f.Close()
		}
	}

	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}

	return nil
}
