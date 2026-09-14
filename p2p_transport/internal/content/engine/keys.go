package engine

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
	"cipher/internal/content/storage"
)

// LocalKeyProvider is an implementation of core.KeyProvider with optional file-backed persistence.
type LocalKeyProvider struct {
	mu    sync.RWMutex
	keys  map[core.ContentID][]byte
	store core.KeyProvider
}

// NewLocalKeyProvider creates a key provider. If dir is provided, it backs key storage with an FSKeyStore.
func NewLocalKeyProvider(dir ...string) *LocalKeyProvider {
	provider := &LocalKeyProvider{
		keys: make(map[core.ContentID][]byte),
	}
	if len(dir) > 0 && dir[0] != "" {
		if ks, err := storage.NewFSKeyStore(dir[0]); err == nil {
			provider.store = ks
		}
	}
	return provider
}

// NewFSKeyStore creates a persistent file key store.
func NewFSKeyStore(dir string) (core.KeyProvider, error) {
	return storage.NewFSKeyStore(dir)
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

	// Double check after lock
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

