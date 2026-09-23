package engine

import (
	"context"
	"encoding/hex"
	"errors"
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

// FSKeyProvider is a disk-backed implementation of core.KeyProvider.
type FSKeyProvider struct {
	mu       sync.RWMutex
	storeDir string
	keys     map[core.ContentID][]byte
}

func NewFSKeyProvider(storeDir string) *FSKeyProvider {
	p := &FSKeyProvider{
		storeDir: storeDir,
		keys:     make(map[core.ContentID][]byte),
	}
	keysDir := filepath.Join(storeDir, "keys")
	if err := os.MkdirAll(keysDir, 0700); err == nil {
		_ = os.Chmod(keysDir, 0700)
		p.loadKeysFromDisk(keysDir)
	}
	return p
}

func (p *FSKeyProvider) loadKeysFromDisk(keysDir string) {
	entries, err := os.ReadDir(keysDir)
	if err != nil {
		return
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

		filePath := filepath.Join(keysDir, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}
		key, err := parseKeyData(data)
		if err == nil && len(key) == 32 {
			p.keys[id] = key
		}
	}
}

func parseKeyData(data []byte) ([]byte, error) {
	str := strings.TrimSpace(string(data))
	if len(str) == 64 {
		b, err := hex.DecodeString(str)
		if err == nil && len(b) == 32 {
			return b, nil
		}
	}
	if len(data) == 32 {
		keyCopy := make([]byte, 32)
		copy(keyCopy, data)
		return keyCopy, nil
	}
	b, err := hex.DecodeString(str)
	if err == nil && len(b) == 32 {
		return b, nil
	}
	return nil, errors.New("invalid key format or length")
}

func (p *FSKeyProvider) keyPath(id core.ContentID) string {
	return filepath.Join(p.storeDir, "keys", hex.EncodeToString(id[:])+".key")
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

	filePath := p.keyPath(id)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.New("key not found")
	}

	key, err = parseKeyData(data)
	if err != nil || len(key) != 32 {
		return nil, errors.New("key not found or invalid on disk")
	}

	p.mu.Lock()
	p.keys[id] = key
	p.mu.Unlock()

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	keysDir := filepath.Join(p.storeDir, "keys")
	if err := os.MkdirAll(keysDir, 0700); err != nil {
		return err
	}
	_ = os.Chmod(keysDir, 0700)

	filePath := p.keyPath(id)
	if err := os.WriteFile(filePath, key, 0600); err != nil {
		return err
	}
	_ = os.Chmod(filePath, 0600)

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.keys, id)
	filePath := p.keyPath(id)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// WriteKeyFile exports a key to disk at path with 0600 permissions.
func WriteKeyFile(path string, key []byte) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	hexStr := strings.TrimSpace(hex.EncodeToString(key)) + "\n"
	if err := os.WriteFile(path, []byte(hexStr), 0600); err != nil {
		return err
	}
	return os.Chmod(path, 0600)
}

// ParseKeyFlags imports a 32-byte key from keyFile or keyHex.
// keyFile takes priority over keyHex if both are provided.
func ParseKeyFlags(keyFile, keyHex string) ([]byte, error) {
	if keyFile != "" {
		data, err := os.ReadFile(keyFile)
		if err != nil {
			return nil, errors.New("failed to read key file: " + err.Error())
		}
		return parseKeyData(data)
	}
	if keyHex != "" {
		str := strings.TrimSpace(keyHex)
		b, err := hex.DecodeString(str)
		if err != nil || len(b) != 32 {
			return nil, errors.New("invalid key hex (must be 32-byte hex)")
		}
		return b, nil
	}
	return nil, nil
}


