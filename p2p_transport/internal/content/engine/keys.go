package engine

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"cipher/internal/content/core"
)

// wipe overwrites secret key bytes with zero values in memory and
// uses runtime.KeepAlive to ensure compiler optimization does not eliminate the loop.
func wipe(b []byte) {
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

// Get returns an isolated copy of the secret key for the specified ContentID.
// The caller is responsible for wiping/zeroing the returned byte slice when finished with it.
func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	key, exists := p.keys[id]
	if !exists {
		return nil, errors.New("key not found")
	}
	// return a copy to prevent mutation and isolate callers
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

// Put stores a copy of the secret key for the specified ContentID.
// If a key already exists for this ContentID, its underlying byte slice is explicitly zeroed before replacement.
func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, exists := p.keys[id]; exists {
		wipe(existing)
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

// Delete removes the secret key for the specified ContentID.
// The underlying byte slice is explicitly zeroed before deleting the map entry.
func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, exists := p.keys[id]; exists {
		wipe(existing)
		delete(p.keys, id)
	}
	return nil
}
