package availability

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	contract "cipher/availability/availability-contracts/contract"
	"cipher/network/content/core"
	"cipher/network/content/manifest"
)

// StorageChunkCountResolver bridges CIPHER's Content Engine (ManifestStore / FSStorage)
// to the Availability contract subsystem (contract.ChunkCountResolver).
type StorageChunkCountResolver struct {
	mu            sync.RWMutex
	manifestStore core.ManifestStore
	inMemory      map[string]*manifest.Manifest
}

// NewStorageChunkCountResolver creates a resolver backed by an optional ManifestStore
// and an in-memory fallback registry.
func NewStorageChunkCountResolver(store core.ManifestStore) *StorageChunkCountResolver {
	return &StorageChunkCountResolver{
		manifestStore: store,
		inMemory:      make(map[string]*manifest.Manifest),
	}
}

// RegisterManifest caches a manifest in-memory under both its ContentID hex string
// and any custom aliases (e.g. filename).
func (r *StorageChunkCountResolver) RegisterManifest(m *manifest.Manifest, aliases ...string) {
	if m == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	contentIDHex := hex.EncodeToString(m.Descriptor.ID[:])
	r.inMemory[contentIDHex] = m
	for _, alias := range aliases {
		if alias != "" {
			r.inMemory[alias] = m
		}
	}
}

// ChunkCount fulfills the contract.ChunkCountResolver interface.
// It resolves the number of chunks for a given fileID from the in-memory cache
// or by reading the manifest from the configured ManifestStore.
func (r *StorageChunkCountResolver) ChunkCount(providerID, fileID string) (int, error) {
	if fileID == "" {
		return 0, errors.New("fileID cannot be empty")
	}

	// 1. Check in-memory cache first
	r.mu.RLock()
	if m, ok := r.inMemory[fileID]; ok {
		r.mu.RUnlock()
		if len(m.ChunkIDs) == 0 {
			return 0, fmt.Errorf("manifest for file %s has 0 chunks", fileID)
		}
		return len(m.ChunkIDs), nil
	}
	r.mu.RUnlock()

	// 2. If a ManifestStore is configured, attempt to decode fileID as a ContentID
	if r.manifestStore != nil {
		ctx := context.Background()
		var contentID core.ContentID

		idBytes, err := hex.DecodeString(fileID)
		if err == nil && len(idBytes) == 32 {
			copy(contentID[:], idBytes)
			data, err := r.manifestStore.GetManifestBytes(ctx, contentID)
			if err == nil {
				m, err := manifest.Deserialize(data)
				if err == nil && len(m.ChunkIDs) > 0 {
					// Cache it in-memory for subsequent lookups
					r.RegisterManifest(m)
					return len(m.ChunkIDs), nil
				}
			}
		}

		// Also check all stored manifests in case fileID matches a filename or custom key
		ids, err := r.manifestStore.ListManifests(ctx)
		if err == nil {
			for _, id := range ids {
				if hex.EncodeToString(id[:]) == fileID {
					data, err := r.manifestStore.GetManifestBytes(ctx, id)
					if err == nil {
						m, err := manifest.Deserialize(data)
						if err == nil && len(m.ChunkIDs) > 0 {
							r.RegisterManifest(m)
							return len(m.ChunkIDs), nil
						}
					}
				}
			}
		}
	}

	return 0, fmt.Errorf("file chunk metadata not found for fileID %q (provider %q)", fileID, providerID)
}

// ConfigureGlobal installs this resolver globally into availability-contracts/contract.
func (r *StorageChunkCountResolver) ConfigureGlobal() error {
	return contract.ConfigureChunkCountResolver(r)
}

// InstallStorageResolver is a convenience helper to create and globally configure
// a StorageChunkCountResolver backed by the given store.
func InstallStorageResolver(store core.ManifestStore) (*StorageChunkCountResolver, error) {
	resolver := NewStorageChunkCountResolver(store)
	if err := resolver.ConfigureGlobal(); err != nil {
		return nil, err
	}
	return resolver, nil
}
