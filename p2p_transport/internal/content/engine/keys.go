package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

// FormatMaskedKey formats a key byte slice as a string. If showKey is false,
// it returns a masked string showing only the first 4 and last 4 hex characters
// (e.g., "a1b2...c3d4"). If showKey is true, it returns the full hexadecimal string.
func FormatMaskedKey(key []byte, showKey bool) string {
	hexStr := hex.EncodeToString(key)
	if showKey {
		return hexStr
	}
	if len(hexStr) <= 8 {
		return hexStr
	}
	return hexStr[:4] + "..." + hexStr[len(hexStr)-4:]
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

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	p.keys[id] = keyCopy
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.keys, id)
	return nil
}
