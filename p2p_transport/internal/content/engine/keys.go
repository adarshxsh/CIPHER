package engine

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"cipher/internal/content/core"
	"golang.org/x/crypto/chacha20poly1305"
)

// LocalKeyProvider is an in-memory and persistent encrypted key store implementation of core.KeyProvider.
type LocalKeyProvider struct {
	mu        sync.RWMutex
	keys      map[core.ContentID][]byte
	storeDir  string
	masterKey []byte
}

// NewLocalKeyProvider creates a key provider. If optional storeDir arguments are provided,
// the first string is used as the directory path for encrypted key persistence.
func NewLocalKeyProvider(opts ...string) *LocalKeyProvider {
	p := &LocalKeyProvider{
		keys: make(map[core.ContentID][]byte),
	}
	if len(opts) > 0 && opts[0] != "" {
		_ = p.InitPersistence(opts[0], nil)
	}
	return p
}

// NewEncryptedKeyProvider creates a persistent key provider storing encrypted keys on disk.
func NewEncryptedKeyProvider(storeDir string, masterKey []byte) (*LocalKeyProvider, error) {
	p := &LocalKeyProvider{
		keys: make(map[core.ContentID][]byte),
	}
	if err := p.InitPersistence(storeDir, masterKey); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *LocalKeyProvider) InitPersistence(storeDir string, masterKey []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := os.MkdirAll(storeDir, 0700); err != nil {
		return fmt.Errorf("failed to create key store dir: %w", err)
	}

	if len(masterKey) == 0 {
		masterKeyFile := filepath.Join(storeDir, ".masterkey")
		data, err := os.ReadFile(masterKeyFile)
		if err == nil && len(data) == chacha20poly1305.KeySize {
			masterKey = data
		} else {
			masterKey = make([]byte, chacha20poly1305.KeySize)
			if _, err := rand.Read(masterKey); err != nil {
				return fmt.Errorf("failed to generate master key: %w", err)
			}
			if err := os.WriteFile(masterKeyFile, masterKey, 0600); err != nil {
				return fmt.Errorf("failed to save master key: %w", err)
			}
		}
	} else if len(masterKey) != chacha20poly1305.KeySize {
		return fmt.Errorf("invalid master key length: expected %d bytes, got %d", chacha20poly1305.KeySize, len(masterKey))
	}

	p.storeDir = storeDir
	p.masterKey = make([]byte, len(masterKey))
	copy(p.masterKey, masterKey)
	return nil
}

func (p *LocalKeyProvider) keyPath(id core.ContentID) string {
	return filepath.Join(p.storeDir, fmt.Sprintf("%x.key.enc", id))
}

func (p *LocalKeyProvider) getAEAD() (cipher.AEAD, error) {
	if len(p.masterKey) == 0 {
		return nil, errors.New("no master key configured")
	}
	return chacha20poly1305.New(p.masterKey)
}

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	key, exists := p.keys[id]
	if exists {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		p.mu.RUnlock()
		return keyCopy, nil
	}
	storeDir := p.storeDir
	p.mu.RUnlock()

	if storeDir == "" {
		return nil, errors.New("key not found")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check memory after acquiring write lock
	if key, exists := p.keys[id]; exists {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}

	path := p.keyPath(id)
	encData, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("key not found")
	}

	aead, err := p.getAEAD()
	if err != nil {
		return nil, fmt.Errorf("crypto error: %w", err)
	}

	nonceSize := aead.NonceSize()
	if len(encData) < nonceSize {
		return nil, errors.New("corrupt key file: truncated nonce")
	}

	nonce := encData[:nonceSize]
	ciphertext := encData[nonceSize:]

	decKey, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt key: %w", err)
	}

	keyCopy := make([]byte, len(decKey))
	copy(keyCopy, decKey)
	p.keys[id] = keyCopy

	return decKey, nil
}

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy

	if p.storeDir != "" {
		aead, err := p.getAEAD()
		if err != nil {
			return fmt.Errorf("crypto error: %w", err)
		}

		nonce := make([]byte, aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return fmt.Errorf("failed to generate nonce: %w", err)
		}

		ciphertext := aead.Seal(nil, nonce, key, nil)
		encData := append(nonce, ciphertext...)

		path := p.keyPath(id)
		if err := os.WriteFile(path, encData, 0600); err != nil {
			return fmt.Errorf("failed to persist key file: %w", err)
		}
	}

	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.keys, id)

	if p.storeDir != "" {
		path := p.keyPath(id)
		_ = os.Remove(path)
	}

	return nil
}
