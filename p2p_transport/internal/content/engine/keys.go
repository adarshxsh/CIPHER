package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// FSKeyProvider is a persistent, file-backed implementation of core.KeyProvider.
// Keys are stored under <storePath>/keys/<hex_content_id>.key with 0600 permissions.
type FSKeyProvider struct {
	storePath string
	mu        sync.RWMutex
	keys      map[core.ContentID][]byte
}

func NewFSKeyProvider(storePath string) (*FSKeyProvider, error) {
	keysDir := filepath.Join(storePath, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create keys directory: %w", err)
	}
	_ = os.Chmod(keysDir, 0700)

	kp := &FSKeyProvider{
		storePath: storePath,
		keys:      make(map[core.ContentID][]byte),
	}

	entries, err := os.ReadDir(keysDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read keys directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".key" {
			continue
		}
		encoded := strings.TrimSuffix(entry.Name(), ".key")
		idBytes, err := hex.DecodeString(encoded)
		if err != nil || len(idBytes) != 32 {
			continue
		}
		var id core.ContentID
		copy(id[:], idBytes)

		keyPath := filepath.Join(keysDir, entry.Name())
		keyData, err := os.ReadFile(keyPath)
		if err != nil {
			continue
		}
		trimmed := strings.TrimSpace(string(keyData))
		if len(trimmed) == 64 {
			kBytes, err := hex.DecodeString(trimmed)
			if err == nil {
				kp.keys[id] = kBytes
				continue
			}
		}
		if len(keyData) == 32 {
			kBytes := make([]byte, 32)
			copy(kBytes, keyData)
			kp.keys[id] = kBytes
		}
	}

	return kp, nil
}

func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key, exists := p.keys[id]
	if !exists {
		// Fallback check on disk
		encoded := hex.EncodeToString(id[:])
		keyPath := filepath.Join(p.storePath, "keys", encoded+".key")
		keyData, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, errors.New("key not found")
		}
		trimmed := strings.TrimSpace(string(keyData))
		var kBytes []byte
		if len(trimmed) == 64 {
			kb, err := hex.DecodeString(trimmed)
			if err == nil {
				kBytes = kb
			}
		} else if len(keyData) == 32 {
			kBytes = make([]byte, 32)
			copy(kBytes, keyData)
		}
		if len(kBytes) == 0 {
			return nil, errors.New("invalid key file content")
		}
		return kBytes, nil
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	keysDir := filepath.Join(p.storePath, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return fmt.Errorf("failed to create keys directory: %w", err)
	}
	_ = os.Chmod(keysDir, 0700)

	encoded := hex.EncodeToString(id[:])
	keyPath := filepath.Join(keysDir, encoded+".key")

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	hexStr := hex.EncodeToString(keyCopy) + "\n"
	if err := os.WriteFile(keyPath, []byte(hexStr), 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	_ = os.Chmod(keyPath, 0600)

	p.keys[id] = keyCopy
	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	encoded := hex.EncodeToString(id[:])
	keyPath := filepath.Join(p.storePath, "keys", encoded+".key")

	if err := os.Remove(keyPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}

	delete(p.keys, id)
	return nil
}

// KeyID calculates a non-sensitive fingerprint string for a key (truncated SHA-256 hash).
func KeyID(key []byte) string {
	if len(key) == 0 {
		return "[none]"
	}
	hash := sha256.Sum256(key)
	return hex.EncodeToString(hash[:8])
}

// LoadKeyFromFile reads a 32-byte key from a file (supporting 64 hex characters or 32 raw bytes).
func LoadKeyFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file %s: %w", path, err)
	}
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) == 64 {
		key, err := hex.DecodeString(trimmed)
		if err != nil {
			return nil, fmt.Errorf("invalid hex in key file %s: %w", path, err)
		}
		return key, nil
	}
	if len(data) == 32 {
		key := make([]byte, 32)
		copy(key, data)
		return key, nil
	}
	return nil, fmt.Errorf("invalid key file %s length (must be 64 hex characters or 32 raw bytes)", path)
}

// ExportKeyToFile writes a key to a file with 0600 permissions.
func ExportKeyToFile(path string, key []byte) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("failed to create directory for key export: %w", err)
		}
	}
	hexStr := hex.EncodeToString(key) + "\n"
	if err := os.WriteFile(path, []byte(hexStr), 0600); err != nil {
		return fmt.Errorf("failed to export key file: %w", err)
	}
	_ = os.Chmod(path, 0600)
	return nil
}

