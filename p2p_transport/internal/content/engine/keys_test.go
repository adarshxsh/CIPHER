package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_Delete_ZeroesMemory(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	id := core.ContentID{1, 2, 3, 4}
	secretKey := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}

	err := provider.Put(ctx, id, secretKey)
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	// Capture reference to underlying slice stored in internal map
	provider.mu.RLock()
	internalSlice := provider.keys[id]
	provider.mu.RUnlock()

	if len(internalSlice) != len(secretKey) {
		t.Fatalf("expected internal slice length %d, got %d", len(secretKey), len(internalSlice))
	}

	// Verify internal slice has non-zero bytes before deletion
	hasNonZero := false
	for _, b := range internalSlice {
		if b != 0 {
			hasNonZero = true
			break
		}
	}
	if !hasNonZero {
		t.Fatalf("expected internal slice to contain secret key bytes before deletion")
	}

	// Delete the key entry
	err = provider.Delete(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error on Delete: %v", err)
	}

	// Verify key is removed from provider
	_, err = provider.Get(ctx, id)
	if err == nil {
		t.Fatalf("expected error getting deleted key, got nil")
	}

	// Verify underlying internal memory slice is 100% zeroed out
	for i, b := range internalSlice {
		if b != 0 {
			t.Errorf("expected byte at index %d to be 0x00 post-deletion, got 0x%02X", i, b)
		}
	}
}

func TestLocalKeyProvider_Put_ZeroesPreviousKeyMemory(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	id := core.ContentID{1, 1, 1, 1}
	key1 := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	key2 := []byte{0x11, 0x22, 0x33, 0x44}

	if err := provider.Put(ctx, id, key1); err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}

	provider.mu.RLock()
	ref1 := provider.keys[id]
	provider.mu.RUnlock()

	// Overwrite key for same ContentID
	if err := provider.Put(ctx, id, key2); err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}

	// Verify previous memory buffer ref1 was zeroed
	for i, b := range ref1 {
		if b != 0 {
			t.Errorf("expected previous key byte at index %d to be 0x00 post-overwrite, got 0x%02X", i, b)
		}
	}

	// Verify new key is returned
	gotKey, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	for i, b := range key2 {
		if gotKey[i] != b {
			t.Errorf("expected byte %d to be 0x%02X, got 0x%02X", i, b, gotKey[i])
		}
	}
}

func TestLocalKeyProvider_Clear_Reset_Close(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	id1 := core.ContentID{1}
	id2 := core.ContentID{2}

	_ = provider.Put(ctx, id1, []byte{0xFF, 0xEE, 0xDD})
	_ = provider.Put(ctx, id2, []byte{0xCC, 0xBB, 0xAA})

	provider.mu.RLock()
	ref1 := provider.keys[id1]
	ref2 := provider.keys[id2]
	provider.mu.RUnlock()

	// Test Clear
	provider.Clear()

	for i, b := range ref1 {
		if b != 0 {
			t.Errorf("ref1 byte %d not zeroed after Clear: 0x%02X", i, b)
		}
	}
	for i, b := range ref2 {
		if b != 0 {
			t.Errorf("ref2 byte %d not zeroed after Clear: 0x%02X", i, b)
		}
	}

	if len(provider.keys) != 0 {
		t.Errorf("expected map length 0 after Clear, got %d", len(provider.keys))
	}

	// Test Reset
	_ = provider.Put(ctx, id1, []byte{0x12, 0x34})
	provider.mu.RLock()
	ref3 := provider.keys[id1]
	provider.mu.RUnlock()

	provider.Reset()
	for i, b := range ref3 {
		if b != 0 {
			t.Errorf("ref3 byte %d not zeroed after Reset: 0x%02X", i, b)
		}
	}

	// Test Close
	_ = provider.Put(ctx, id1, []byte{0x56, 0x78})
	provider.mu.RLock()
	ref4 := provider.keys[id1]
	provider.mu.RUnlock()

	_ = provider.Close()
	for i, b := range ref4 {
		if b != 0 {
			t.Errorf("ref4 byte %d not zeroed after Close: 0x%02X", i, b)
		}
	}
}

func TestLocalKeyProvider_Concurrency(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				id := core.ContentID{byte(workerID), byte(j)}
				key := []byte(fmt.Sprintf("key-%d-%d", workerID, j))

				_ = provider.Put(ctx, id, key)
				_, _ = provider.Get(ctx, id)
				_ = provider.Delete(ctx, id)
			}
		}(i)
	}

	wg.Wait()
	provider.Clear()
}
