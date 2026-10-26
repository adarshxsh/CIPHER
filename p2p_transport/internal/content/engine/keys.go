package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// FileKeyVault is a persistent implementation of core.KeyProvider that stores
// ContentID-to-Key mappings in keys.json with restricted 0600 permissions.
type FileKeyVault struct {
	mu       sync.RWMutex
	filePath string
	keys     map[core.ContentID][]byte
}

// NewFileKeyVault creates a new persistent key vault.
// path can be the directory containing keys.json or the full path to keys.json.
func NewFileKeyVault(path string) (*FileKeyVault, error) {
	filePath := path
	if filePath == "" {
		filePath = "./store/keys.json"
	} else if !strings.HasSuffix(filePath, ".json") {
		filePath = filepath.Join(filePath, "keys.json")
	}

	vault := &FileKeyVault{
		filePath: filePath,
		keys:     make(map[core.ContentID][]byte),
	}

	if err := vault.Load(); err != nil {
		return nil, err
	}

	return vault, nil
}

// FilePath returns the absolute or configured file path of the keys.json store.
func (v *FileKeyVault) FilePath() string {
	return v.filePath
}

// Load reads ContentID-to-Key mappings from the keys.json store file.
func (v *FileKeyVault) Load() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	data, err := os.ReadFile(v.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read key vault file: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	var jsonMap map[string]string
	if err := json.Unmarshal(data, &jsonMap); err != nil {
		return fmt.Errorf("failed to parse key vault JSON: %w", err)
	}

	v.keys = make(map[core.ContentID][]byte, len(jsonMap))
	for idHex, keyHex := range jsonMap {
		idBytes, err := hex.DecodeString(idHex)
		if err != nil || len(idBytes) != 32 {
			continue
		}
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil || len(keyBytes) != 32 {
			continue
		}
		var contentID core.ContentID
		copy(contentID[:], idBytes)

		keyCopy := make([]byte, len(keyBytes))
		copy(keyCopy, keyBytes)
		v.keys[contentID] = keyCopy
	}

	return nil
}

// saveLocked persists the in-memory keys to keys.json using atomic temp file writes and 0600 permissions.
func (v *FileKeyVault) saveLocked() error {
	dir := filepath.Dir(v.filePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create key vault directory: %w", err)
	}

	jsonMap := make(map[string]string, len(v.keys))
	for id, key := range v.keys {
		idHex := hex.EncodeToString(id[:])
		keyHex := hex.EncodeToString(key)
		jsonMap[idHex] = keyHex
	}

	data, err := json.MarshalIndent(jsonMap, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal key vault JSON: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d", v.filePath, time.Now().UnixNano())

	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temporary key vault file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("failed to write key vault data: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("failed to sync key vault file: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to close temporary key vault file: %w", err)
	}

	if err := os.Chmod(tmpFile, 0600); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to chmod temporary key vault file: %w", err)
	}

	if err := os.Rename(tmpFile, v.filePath); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to replace key vault file: %w", err)
	}

	if err := os.Chmod(v.filePath, 0600); err != nil {
		return fmt.Errorf("failed to chmod key vault file: %w", err)
	}

	return nil
}

// Get retrieves a key copy for the specified ContentID.
func (v *FileKeyVault) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	v.mu.RLock()
	key, exists := v.keys[id]
	if !exists {
		v.mu.RUnlock()
		return nil, errors.New("key not found")
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	v.mu.RUnlock()
	return keyCopy, nil
}

// Put stores a key for the specified ContentID and atomically writes it to keys.json.
func (v *FileKeyVault) Put(ctx context.Context, id core.ContentID, key []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	v.keys[id] = keyCopy

	return v.saveLocked()
}

// Delete removes a key for the specified ContentID and updates keys.json.
func (v *FileKeyVault) Delete(ctx context.Context, id core.ContentID) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if _, exists := v.keys[id]; !exists {
		return nil
	}

	delete(v.keys, id)
	return v.saveLocked()
}

