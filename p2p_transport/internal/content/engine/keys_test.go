package engine

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_GetIsolation(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	var id core.ContentID
	id[0] = 0x01

	secret := []byte("super-secret-key-32-bytes-long!!")
	if err := provider.Put(ctx, id, secret); err != nil {
		t.Fatalf("unexpected Put error: %v", err)
	}

	retrieved, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected Get error: %v", err)
	}

	if !bytes.Equal(retrieved, secret) {
		t.Fatalf("got key %v, expected %v", retrieved, secret)
	}

	// Mutate retrieved copy
	for i := range retrieved {
		retrieved[i] = 0xFF
	}

	// Fetch again and verify internal state was not modified
	retrieved2, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected Get error: %v", err)
	}

	if !bytes.Equal(retrieved2, secret) {
		t.Fatalf("internal key was mutated; got %v, expected %v", retrieved2, secret)
	}
}

func TestLocalKeyProvider_PutOverwriteWipe(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	var id core.ContentID
	id[0] = 0x02

	key1 := bytes.Repeat([]byte{0xAA}, 32)
	if err := provider.Put(ctx, id, key1); err != nil {
		t.Fatalf("unexpected Put error: %v", err)
	}

	// Hold direct reference to underlying slice stored in provider
	provider.mu.RLock()
	storedSliceRef := provider.keys[id]
	provider.mu.RUnlock()

	key2 := bytes.Repeat([]byte{0xBB}, 32)
	if err := provider.Put(ctx, id, key2); err != nil {
		t.Fatalf("unexpected Put error on overwrite: %v", err)
	}

	// Verify that the previous underlying byte slice was zeroed out in memory
	for i, b := range storedSliceRef {
		if b != 0 {
			t.Errorf("byte at index %d was not wiped on overwrite; got 0x%02x, expected 0x00", i, b)
		}
	}

	// Verify Get returns key2
	retrieved, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected Get error: %v", err)
	}
	if !bytes.Equal(retrieved, key2) {
		t.Fatalf("got key %v, expected %v", retrieved, key2)
	}
}

func TestLocalKeyProvider_DeleteWipe(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	var id core.ContentID
	id[0] = 0x03

	key := bytes.Repeat([]byte{0xCC}, 32)
	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("unexpected Put error: %v", err)
	}

	// Hold direct reference to underlying slice stored in provider
	provider.mu.RLock()
	storedSliceRef := provider.keys[id]
	provider.mu.RUnlock()

	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("unexpected Delete error: %v", err)
	}

	// Verify that stored byte slice was zeroed out in memory
	for i, b := range storedSliceRef {
		if b != 0 {
			t.Errorf("byte at index %d was not wiped on delete; got 0x%02x, expected 0x00", i, b)
		}
	}

	// Verify Get returns error after deletion
	_, err := provider.Get(ctx, id)
	if err == nil {
		t.Fatalf("expected error getting deleted key, got nil")
	}
}

func TestLocalKeyProvider_ConcurrentAccess(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var id core.ContentID
			id[0] = byte(workerID)

			key := bytes.Repeat([]byte{byte(workerID)}, 32)

			for j := 0; j < iterations; j++ {
				_ = provider.Put(ctx, id, key)
				_, _ = provider.Get(ctx, id)
				_ = provider.Delete(ctx, id)
			}
		}(i)
	}

	wg.Wait()
}
