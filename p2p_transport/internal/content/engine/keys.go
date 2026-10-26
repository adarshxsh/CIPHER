package engine

import (
	"context"
	"errors"
	"runtime"
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

// Wipe explicitly overwrites all bytes in the slice with zero values and uses
// runtime.KeepAlive to prevent compiler optimizations from eliding the zeroing operation.
func Wipe(b []byte) {
	if b == nil {
		return
	}
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}

// Get retrieves a copy of the key for the specified ContentID.
// Callers must ensure transient key copies are wiped (e.g. using Wipe(key) or defer Wipe(key))
// after cryptographic processing.
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

// Put stores key for ContentID, zeroing any existing key byte slice prior to replacement.
func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, exists := p.keys[id]; exists {
		Wipe(existing)
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

// Delete overwrites the target key byte slice with zero bytes before removing the entry from the map.
func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, exists := p.keys[id]; exists {
		Wipe(existing)
		delete(p.keys, id)
	}
	return nil
}

// Close zeroes all managed key buffers and clears the internal map.
func (p *LocalKeyProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, key := range p.keys {
		Wipe(key)
		delete(p.keys, id)
	}
	return nil
}

// Clear zeroes all managed key buffers and clears the internal map.
func (p *LocalKeyProvider) Clear() error {
	return p.Close()
}
