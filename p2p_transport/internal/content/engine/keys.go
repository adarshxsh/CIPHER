package engine

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrKeyProviderClosed = errors.New("key provider closed")
)

// Zeroize explicitly overwrites a byte slice with zeros to wipe key material from memory.
func Zeroize(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// LocalKeyProvider is an in-memory implementation of core.KeyProvider.
type LocalKeyProvider struct {
	mu     sync.RWMutex
	keys   map[core.ContentID][]byte
	closed bool
}

func NewLocalKeyProvider() *LocalKeyProvider {
	return &LocalKeyProvider{
		keys: make(map[core.ContentID][]byte),
	}
}

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrKeyProviderClosed
	}
	key, exists := p.keys[id]
	if !exists {
		return nil, ErrKeyNotFound
	}
	// return a copy to prevent mutation
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrKeyProviderClosed
	}
	if oldKey, exists := p.keys[id]; exists {
		Zeroize(oldKey)
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrKeyProviderClosed
	}
	if key, exists := p.keys[id]; exists {
		Zeroize(key)
		delete(p.keys, id)
	}
	return nil
}

func (p *LocalKeyProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	for id, key := range p.keys {
		Zeroize(key)
		delete(p.keys, id)
	}
	p.closed = true
	return nil
}
