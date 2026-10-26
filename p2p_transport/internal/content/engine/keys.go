package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	"cipher/internal/content/core"
	"cipher/internal/content/storage"
)

// LocalKeyProvider is an implementation of core.KeyProvider with optional persistent keystore backing.
type LocalKeyProvider struct {
	mu    sync.RWMutex
	keys  map[core.ContentID][]byte
	store core.KeyProvider
}

// KeyManager is an alias for LocalKeyProvider to support KeyManager terminology.
type KeyManager = LocalKeyProvider

// NewLocalKeyProvider creates a key provider. If dir is provided (or if CIPHER_CONFIG_DIR is set),
// it backs key storage with an encrypted FSKeyStore.
func NewLocalKeyProvider(dir ...string) *LocalKeyProvider {
	provider := &LocalKeyProvider{
		keys: make(map[core.ContentID][]byte),
	}
	baseDir := ""
	if len(dir) > 0 {
		baseDir = dir[0]
	}
	if ks, err := storage.NewFSKeyStore(baseDir); err == nil {
		provider.store = ks
	}
	return provider
}

// NewFSKeyStore creates a persistent file key store.
func NewFSKeyStore(dir string) (core.KeyProvider, error) {
	return storage.NewFSKeyStore(dir)
}

// Fingerprint returns a redacted key fingerprint suitable for logging.
func Fingerprint(key []byte) string {
	if len(key) == 0 {
		return "[REDACTED]"
	}
	h := sha256.Sum256(key)
	return hex.EncodeToString(h[:8])
}

// RedactedKey returns a redacted representation string.
func RedactedKey(key []byte) string {
	return "[REDACTED]"
}

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	key, exists := p.keys[id]
	p.mu.RUnlock()

	if exists {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double check after acquiring write lock
	if key, exists := p.keys[id]; exists {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		return keyCopy, nil
	}

	if p.store != nil {
		storedKey, err := p.store.Get(ctx, id)
		if err == nil {
			p.keys[id] = storedKey
			keyCopy := make([]byte, len(storedKey))
			copy(keyCopy, storedKey)
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

	if p.store != nil {
		if err := p.store.Put(ctx, id, key); err != nil {
			return err
		}
	}
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.keys, id)
	if p.store != nil {
		_ = p.store.Delete(ctx, id)
	}
	return nil
}
