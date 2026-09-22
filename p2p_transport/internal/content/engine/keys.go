package engine

import (
	"context"
	"errors"
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

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) (*core.KeyHandle, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	key, exists := p.keys[id]
	if !exists {
		return nil, errors.New("key not found")
	}
	return core.NewKeyHandle(key), nil
}

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if oldKey, exists := p.keys[id]; exists {
		core.ZeroBytes(oldKey)
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if key, exists := p.keys[id]; exists {
		core.ZeroBytes(key)
		delete(p.keys, id)
	}
	return nil
}

// Close zeroes all stored keys in memory and clears the key map.
func (p *LocalKeyProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, key := range p.keys {
		core.ZeroBytes(key)
		delete(p.keys, id)
	}
	return nil
}
