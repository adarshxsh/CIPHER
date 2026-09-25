package engine

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"cipher/internal/content/core"
)

// Wipe overwrites a byte slice with zeroes and ensures compiler optimizations do not eliminate the zeroing.
func Wipe(b []byte) {
	if len(b) == 0 {
		return
	}
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}

// LocalKeyProvider is an in-memory implementation of core.KeyProvider backed by zeroize-capable ProtectedBuffer containers.
type LocalKeyProvider struct {
	mu   sync.RWMutex
	keys map[core.ContentID]*ProtectedBuffer
}

func NewLocalKeyProvider() *LocalKeyProvider {
	return &LocalKeyProvider{
		keys: make(map[core.ContentID]*ProtectedBuffer),
	}
}

// Get retrieves a copy of the key bytes for backward compatibility.
func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	var res []byte
	err := p.WithKey(ctx, id, func(key []byte) error {
		res = make([]byte, len(key))
		copy(res, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Put stores a key in a ProtectedBuffer container backed by page locking and memory zeroing routines.
func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if oldBuf, exists := p.keys[id]; exists && oldBuf != nil {
		oldBuf.Destroy()
	}

	p.keys[id] = NewProtectedBuffer(key)
	return nil
}

// Delete explicitely zeroes key memory before removing the key entry from the map.
func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	buf, exists := p.keys[id]
	if !exists || buf == nil {
		return errors.New("key not found")
	}

	buf.Destroy()
	delete(p.keys, id)
	return nil
}

// WithKey executes a closure routine with temporary access to the key.
// The temporary key buffer is automatically zeroed when the callback returns or panics.
func (p *LocalKeyProvider) WithKey(ctx context.Context, id core.ContentID, fn func(key []byte) error) error {
	p.mu.RLock()
	buf, exists := p.keys[id]
	if !exists || buf == nil || buf.IsClosed() {
		p.mu.RUnlock()
		return errors.New("key not found")
	}

	raw := buf.Bytes()
	if raw == nil {
		p.mu.RUnlock()
		return errors.New("key not found")
	}

	keyCopy := make([]byte, len(raw))
	copy(keyCopy, raw)
	p.mu.RUnlock()

	defer Wipe(keyCopy)
	return fn(keyCopy)
}

// Close zeroes and releases all key buffers stored in the provider.
func (p *LocalKeyProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, buf := range p.keys {
		if buf != nil {
			buf.Destroy()
		}
		delete(p.keys, id)
	}
	return nil
}
