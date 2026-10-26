package engine

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"cipher/internal/content/core"
)

// Wipe overwrites all bytes in the slice with zeroes to erase secret data from memory.
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

func NewLocalKeyProvider() *LocalKeyProvider {
	return &LocalKeyProvider{
		keys: make(map[core.ContentID][]byte),
	}
}

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.keys == nil {
		return nil, errors.New("key not found")
	}
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
	if p.keys == nil {
		p.keys = make(map[core.ContentID][]byte)
	}
	if oldKey, exists := p.keys[id]; exists {
		Wipe(oldKey)
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.keys == nil {
		return nil
	}
	if key, exists := p.keys[id]; exists {
		Wipe(key)
		delete(p.keys, id)
	}
	return nil
}

// Clear zeroes all stored key byte buffers in memory and clears the map.
func (p *LocalKeyProvider) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.keys == nil {
		return
	}
	for id, key := range p.keys {
		Wipe(key)
		delete(p.keys, id)
	}
}

// Reset zeroes all stored key byte buffers in memory and clears the map.
func (p *LocalKeyProvider) Reset() {
	p.Clear()
}

// Close zeroes all stored key byte buffers in memory upon provider shutdown.
func (p *LocalKeyProvider) Close() error {
	p.Clear()
	return nil
}

