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
	keys map[core.ContentID]*core.SecretKey
}

func NewLocalKeyProvider() *LocalKeyProvider {
	return &LocalKeyProvider{
		keys: make(map[core.ContentID]*core.SecretKey),
	}
}

func (p *LocalKeyProvider) Get(ctx context.Context, id core.ContentID) (*core.SecretKey, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	key, exists := p.keys[id]
	if !exists || key == nil {
		return nil, errors.New("key not found")
	}
	return key.Clone()
}

func (p *LocalKeyProvider) Put(ctx context.Context, id core.ContentID, key *core.SecretKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.keys[id]; ok && existing != nil {
		existing.Destroy()
	}
	cloned, err := key.Clone()
	if err != nil {
		return err
	}
	p.keys[id] = cloned
	return nil
}

func (p *LocalKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if key, exists := p.keys[id]; exists {
		if key != nil {
			key.Destroy()
		}
		delete(p.keys, id)
	}
	return nil
}

func (p *LocalKeyProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, key := range p.keys {
		if key != nil {
			key.Destroy()
		}
		delete(p.keys, id)
	}
	return nil
}
