package engine

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

// Wipe overwrites a byte slice with zeros to clear sensitive key material from memory.
func Wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// LocalKeyHandle wraps a copy of key material and zeroes its memory buffer upon release.
type LocalKeyHandle struct {
	mu  sync.Mutex
	key []byte
}

func NewKeyHandle(key []byte) core.KeyHandle {
	if key == nil {
		return &LocalKeyHandle{}
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return &LocalKeyHandle{key: keyCopy}
}

func (h *LocalKeyHandle) Bytes() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.key
}

func (h *LocalKeyHandle) Release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.key != nil {
		Wipe(h.key)
		h.key = nil
	}
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

func (p *LocalKeyProvider) GetHandle(ctx context.Context, id core.ContentID) (core.KeyHandle, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	key, exists := p.keys[id]
	if !exists {
		return nil, errors.New("key not found")
	}
	return NewKeyHandle(key), nil
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
	if key, exists := p.keys[id]; exists {
		Wipe(key)
		delete(p.keys, id)
	}
	return nil
}
