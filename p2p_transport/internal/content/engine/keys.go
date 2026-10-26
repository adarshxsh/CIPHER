package engine

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	"cipher/internal/content/core"
)

// Wipe overwrites byte slices with zeros prior to removal or GC to prevent lingering secrets in memory.
func Wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}

// LocalKeyProvider is an in-memory implementation of core.KeyProvider.
type LocalKeyProvider struct {
	mu   sync.RWMutex
	keys map[core.ContentID][]byte
}

func NewLocalKeyProvider(args ...string) *LocalKeyProvider {
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
	if old, exists := p.keys[id]; exists {
		Wipe(old)
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, exists := p.keys[id]; exists {
		Wipe(old)
	}
	delete(p.keys, id)
	return nil
}

// DefaultKeystorePath resolves the default path to ~/.config/cipher/keystore.db.
func DefaultKeystorePath() string {
	configDir := os.Getenv("CIPHER_CONFIG_DIR")
	if configDir == "" {
		var err error
		configDir, err = os.UserConfigDir()
		if err != nil || configDir == "" {
			homeDir, err := os.UserHomeDir()
			if err == nil {
				configDir = filepath.Join(homeDir, ".config")
			} else {
				configDir = "."
			}
		}
	}
	return filepath.Join(configDir, "cipher", "keystore.db")
}

// FileKeyProvider is an alias for EncryptedFileKeyProvider.
type FileKeyProvider = EncryptedFileKeyProvider

// NewFileKeyProvider constructs a FileKeyProvider (alias for EncryptedFileKeyProvider).
func NewFileKeyProvider(path string, masterKey ...[]byte) (*EncryptedFileKeyProvider, error) {
	return NewEncryptedFileKeyProvider(path, masterKey...)
}

// EncryptedFileKeyProvider is an AES-GCM encrypted file-backed implementation of core.KeyProvider.
type EncryptedFileKeyProvider struct {
	mu        sync.RWMutex
	path      string
	lockPath  string
	masterKey []byte
	keys      map[core.ContentID][]byte
}

// NewEncryptedFileKeyProvider creates or loads an AES-GCM encrypted local key store.
// If path is empty, DefaultKeystorePath() (~/.config/cipher/keystore.db) is used.
func NewEncryptedFileKeyProvider(path string, masterKey ...[]byte) (*EncryptedFileKeyProvider, error) {
	if path == "" {
		path = DefaultKeystorePath()
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create keystore directory: %w", err)
	}
	_ = os.Chmod(dir, 0700)

	var keyBytes []byte
	if len(masterKey) > 0 && len(masterKey[0]) > 0 {
		if len(masterKey[0]) == 32 {
			keyBytes = make([]byte, 32)
			copy(keyBytes, masterKey[0])
		} else {
			hash := sha256.Sum256(masterKey[0])
			keyBytes = hash[:]
		}
	} else if envKey := os.Getenv("CIPHER_KEYSTORE_KEY"); envKey != "" {
		if decoded, err := hex.DecodeString(envKey); err == nil && len(decoded) == 32 {
			keyBytes = decoded
		} else {
			hash := sha256.Sum256([]byte(envKey))
			keyBytes = hash[:]
		}
	} else {
		masterKeyPath := filepath.Join(dir, "keystore.key")
		if data, err := os.ReadFile(masterKeyPath); err == nil && len(data) == 32 {
			keyBytes = data
		} else {
			keyBytes = make([]byte, 32)
			if _, err := rand.Read(keyBytes); err != nil {
				return nil, fmt.Errorf("failed to generate master key: %w", err)
			}
			if err := os.WriteFile(masterKeyPath, keyBytes, 0600); err != nil {
				return nil, fmt.Errorf("failed to write master key: %w", err)
			}
			_ = os.Chmod(masterKeyPath, 0600)
		}
	}

	p := &EncryptedFileKeyProvider{
		path:      path,
		lockPath:  path + ".lock",
		masterKey: keyBytes,
		keys:      make(map[core.ContentID][]byte),
	}

	if err := p.loadLocked(); err != nil {
		return nil, fmt.Errorf("failed to load encrypted keystore: %w", err)
	}

	return p, nil
}

func (p *EncryptedFileKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	key, exists := p.keys[id]
	p.mu.RUnlock()

	if exists {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}

	// Try reloading from disk if not found in memory map
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.loadLocked(); err == nil {
		if key, exists := p.keys[id]; exists {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			return keyCopy, nil
		}
	}

	return nil, errors.New("key not found")
}

func (p *EncryptedFileKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if old, exists := p.keys[id]; exists {
		Wipe(old)
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy

	return p.saveLocked()
}

func (p *EncryptedFileKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if old, exists := p.keys[id]; exists {
		Wipe(old)
		delete(p.keys, id)
		return p.saveLocked()
	}

	return nil
}

func (p *EncryptedFileKeyProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, key := range p.keys {
		Wipe(key)
	}
	p.keys = make(map[core.ContentID][]byte)
	Wipe(p.masterKey)
	return nil
}

func (p *EncryptedFileKeyProvider) lockFile() (*os.File, error) {
	f, err := os.OpenFile(p.lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (p *EncryptedFileKeyProvider) unlockFile(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}

func (p *EncryptedFileKeyProvider) saveLocked() error {
	lockFile, err := p.lockFile()
	if err != nil {
		return fmt.Errorf("failed to acquire file lock: %w", err)
	}
	defer p.unlockFile(lockFile)

	dir := filepath.Dir(p.path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	_ = os.Chmod(dir, 0700)

	exportMap := make(map[string]string)
	for id, key := range p.keys {
		exportMap[hex.EncodeToString(id[:])] = hex.EncodeToString(key)
	}

	jsonBytes, err := json.Marshal(exportMap)
	if err != nil {
		return fmt.Errorf("failed to marshal keys: %w", err)
	}

	block, err := aes.NewCipher(p.masterKey)
	if err != nil {
		return fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("failed to create GCM cipher: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, jsonBytes, nil)

	tmpPath := p.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := tmpFile.Write(ciphertext); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write encrypted data: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	tmpFile.Close()

	if err := os.Chmod(tmpPath, 0600); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to set chmod on temp file: %w", err)
	}

	if err := os.Rename(tmpPath, p.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file to keystore: %w", err)
	}

	_ = os.Chmod(p.path, 0600)
	return nil
}

func (p *EncryptedFileKeyProvider) loadLocked() error {
	lockFile, err := p.lockFile()
	if err != nil {
		return fmt.Errorf("failed to acquire file lock: %w", err)
	}
	defer p.unlockFile(lockFile)

	if _, err := os.Stat(p.path); os.IsNotExist(err) {
		if p.keys == nil {
			p.keys = make(map[core.ContentID][]byte)
		}
		return nil
	}

	data, err := os.ReadFile(p.path)
	if err != nil {
		return fmt.Errorf("failed to read keystore file: %w", err)
	}

	block, err := aes.NewCipher(p.masterKey)
	if err != nil {
		return fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("failed to create GCM cipher: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return errors.New("keystore file corrupted or truncated")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	jsonBytes, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return fmt.Errorf("failed to decrypt keystore data: %w", err)
	}

	var importMap map[string]string
	if err := json.Unmarshal(jsonBytes, &importMap); err != nil {
		return fmt.Errorf("failed to unmarshal keystore JSON: %w", err)
	}

	newKeys := make(map[core.ContentID][]byte)
	for idHex, keyHex := range importMap {
		idBytes, err := hex.DecodeString(idHex)
		if err != nil || len(idBytes) != 32 {
			continue
		}
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			continue
		}
		var cid core.ContentID
		copy(cid[:], idBytes)
		newKeys[cid] = keyBytes
	}

	// Wipe existing keys in p.keys before replacing
	for _, old := range p.keys {
		Wipe(old)
	}
	p.keys = newKeys

	return nil
}
