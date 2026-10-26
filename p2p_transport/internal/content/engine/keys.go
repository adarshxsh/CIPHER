package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cipher/internal/content/core"
)

// MaskKey masks raw key material into a safe display fingerprint.
func MaskKey(key []byte) string {
	if len(key) == 0 {
		return "[REDACTED]"
	}
	hexStr := hex.EncodeToString(key)
	if len(hexStr) >= 8 {
		return fmt.Sprintf("%s...%s", hexStr[:4], hexStr[len(hexStr)-4:])
	}
	return "[REDACTED]"
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
	keysDir string
	keys    map[core.ContentID][]byte
}

func NewFSKeyProvider(baseDir string) (*FSKeyProvider, error) {
	if baseDir == "" {
		baseDir = "."
	}
	keysDir := filepath.Join(baseDir, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create keys directory: %w", err)
	}
	_ = os.Chmod(keysDir, 0700)

	provider := &FSKeyProvider{
		baseDir: baseDir,
		keysDir: keysDir,
		keys:    make(map[core.ContentID][]byte),
	}

	if err := provider.indexKeys(); err != nil {
		return nil, fmt.Errorf("failed to index existing keys: %w", err)
	}

	return provider, nil
}

func (p *FSKeyProvider) indexKeys() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	entries, err := os.ReadDir(p.keysDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".key") {
			continue
		}
		hexID := strings.TrimSuffix(entry.Name(), ".key")
		idBytes, err := hex.DecodeString(hexID)
		if err != nil || len(idBytes) != 32 {
			continue
		}
		var id core.ContentID
		copy(id[:], idBytes)

		filePath := filepath.Join(p.keysDir, entry.Name())
		_ = os.Chmod(filePath, 0600)
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		keyBytes := data
		if len(data) == 64 {
			if decoded, err := hex.DecodeString(string(data)); err == nil && len(decoded) == 32 {
				keyBytes = decoded
			}
		}

		keyCopy := make([]byte, len(keyBytes))
		copy(keyCopy, keyBytes)
		p.keys[id] = keyCopy
	}

	return nil
}

func (p *FSKeyProvider) KeyPath(id core.ContentID) string {
	hexID := hex.EncodeToString(id[:])
	return filepath.Join(p.keysDir, hexID+".key")
}

func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	key, exists := p.keys[id]
	if exists {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		p.mu.RUnlock()
		return keyCopy, nil
	}
	p.mu.RUnlock()

	filePath := p.KeyPath(id)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.New("key not found")
	}

	keyBytes := data
	if len(data) == 64 {
		if decoded, err := hex.DecodeString(string(data)); err == nil && len(decoded) == 32 {
			keyBytes = decoded
		}
	}

	p.mu.Lock()
	keyCopy := make([]byte, len(keyBytes))
	copy(keyCopy, keyBytes)
	p.keys[id] = keyCopy
	p.mu.Unlock()

	resCopy := make([]byte, len(keyBytes))
	copy(resCopy, keyBytes)
	return resCopy, nil
}

func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := os.MkdirAll(p.keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys directory: %w", err)
	}
	_ = os.Chmod(p.keysDir, 0700)

	targetPath := p.KeyPath(id)
	var randBuf [8]byte
	_, _ = rand.Read(randBuf[:])
	tmpPath := fmt.Sprintf("%s.%d_%x.tmp", targetPath, os.Getpid(), randBuf)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp key file: %w", err)
	}

	if _, err := f.Write(key); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write key material: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync key file: %w", err)
	}
	f.Close()

	if err := os.Chmod(tmpPath, 0600); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to set key file permissions: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to atomic rename key file: %w", err)
	}

	_ = os.Chmod(targetPath, 0600)

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy

	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.keys, id)
	filePath := p.KeyPath(id)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

