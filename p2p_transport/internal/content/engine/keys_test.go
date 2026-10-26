package engine

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func TestZeroizeHelper(t *testing.T) {
	data := []byte("secret-key-material-123456789012")
	Zeroize(data)
	for i, b := range data {
		if b != 0 {
			t.Fatalf("expected byte at index %d to be 0, got %d", i, b)
		}
	}
}

func TestLocalKeyProvider_ZeroizationOnDelete(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	id := core.ContentID{1, 2, 3, 4, 5}
	key := []byte("super-secret-decryption-key-32b!")

	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// Capture reference to underlying key slice inside provider
	provider.mu.RLock()
	internalSlice, exists := provider.keys[id]
	provider.mu.RUnlock()

	if !exists {
		t.Fatalf("key was not stored in provider map")
	}

	if bytes.Equal(internalSlice, make([]byte, len(key))) {
		t.Fatalf("stored key slice should not be zero prior to deletion")
	}

	// Delete key
	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	// Assert key material in memory was zeroed
	for i, b := range internalSlice {
		if b != 0 {
			t.Errorf("byte at index %d was not zeroized after Delete: got %d", i, b)
		}
	}

	// Assert Get returns key not found
	_, err := provider.Get(ctx, id)
	if err == nil {
		t.Fatalf("expected error getting deleted key, got nil")
	}
}

func TestLocalKeyProvider_ZeroizationOnClose(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	id1 := core.ContentID{1, 1, 1}
	id2 := core.ContentID{2, 2, 2}
	key1 := []byte("secret-key-one-32-bytes-long!!!")
	key2 := []byte("secret-key-two-32-bytes-long!!!")

	if err := provider.Put(ctx, id1, key1); err != nil {
		t.Fatalf("failed to put key1: %v", err)
	}
	if err := provider.Put(ctx, id2, key2); err != nil {
		t.Fatalf("failed to put key2: %v", err)
	}

	provider.mu.RLock()
	slice1 := provider.keys[id1]
	slice2 := provider.keys[id2]
	provider.mu.RUnlock()

	if err := provider.Close(); err != nil {
		t.Fatalf("failed to close key provider: %v", err)
	}

	// Assert all slices zeroized
	for i, b := range slice1 {
		if b != 0 {
			t.Errorf("slice1 byte at %d was not zeroized on Close: got %d", i, b)
		}
	}
	for i, b := range slice2 {
		if b != 0 {
			t.Errorf("slice2 byte at %d was not zeroized on Close: got %d", i, b)
		}
	}

	// Assert operations fail after Close
	if _, err := provider.Get(ctx, id1); err != ErrKeyProviderClosed {
		t.Errorf("expected ErrKeyProviderClosed on Get, got %v", err)
	}
	if err := provider.Put(ctx, id1, key1); err != ErrKeyProviderClosed {
		t.Errorf("expected ErrKeyProviderClosed on Put, got %v", err)
	}
	if err := provider.Delete(ctx, id1); err != ErrKeyProviderClosed {
		t.Errorf("expected ErrKeyProviderClosed on Delete, got %v", err)
	}

	// Repeated Close should be no-op
	if err := provider.Close(); err != nil {
		t.Errorf("expected nil on repeated Close, got %v", err)
	}
}

func TestLocalKeyProvider_ZeroizationOnOverwrite(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	id := core.ContentID{9, 9, 9}
	originalKey := []byte("original-key-32-bytes-long!!!!!")
	newKey := []byte("new-secret-key-32-bytes-long!!!")

	if err := provider.Put(ctx, id, originalKey); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	provider.mu.RLock()
	originalSlice := provider.keys[id]
	provider.mu.RUnlock()

	// Overwrite key
	if err := provider.Put(ctx, id, newKey); err != nil {
		t.Fatalf("failed to overwrite key: %v", err)
	}

	// Verify original slice zeroized
	for i, b := range originalSlice {
		if b != 0 {
			t.Errorf("original slice byte at %d was not zeroized on overwrite: got %d", i, b)
		}
	}

	// Verify Get returns copy of new key
	got, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("failed to get new key: %v", err)
	}
	if !bytes.Equal(got, newKey) {
		t.Errorf("expected new key %x, got %x", newKey, got)
	}
}

func TestLocalKeyProvider_Concurrency(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var id core.ContentID
			id[0] = byte(idx)
			key := make([]byte, 32)
			key[0] = byte(idx)

			_ = provider.Put(ctx, id, key)
			_, _ = provider.Get(ctx, id)
			_ = provider.Delete(ctx, id)
		}(i)
	}
	wg.Wait()
	_ = provider.Close()
}
