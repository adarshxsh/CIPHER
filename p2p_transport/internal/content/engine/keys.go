package engine

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

// Zeroize overwrites the provided byte slice with zero bytes to clear key material from memory.
func Zeroize(b []byte) {
	core.Zeroize(b)
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

func (p *LocalKeyProvider) GetHandle(ctx context.Context, id core.ContentID) (*core.KeyHandle, error) {
	keyBytes, err := p.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return core.NewKeyHandle(keyBytes), nil
}

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, exists := p.keys[id]; exists {
		Zeroize(existing)
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
		Zeroize(key)
		delete(p.keys, id)
	}
	return nil
}

func (p *LocalKeyProvider) Release(ctx context.Context, id core.ContentID) error {
	return p.Delete(ctx, id)
}
