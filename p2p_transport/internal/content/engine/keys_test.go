package engine

import (
	"bytes"
	"context"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_DefensiveCopyingAndZeroing(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	id := core.ContentID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	originalKey := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04}

	inputKey := make([]byte, len(originalKey))
	copy(inputKey, originalKey)

	// 1. Test Put and defensive copying on Put
	if err := provider.Put(ctx, id, inputKey); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Mutate input key slice to ensure provider made a copy
	inputKey[0] = 0x00
	fetched1, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(fetched1, originalKey) {
		t.Errorf("Get returned %v, expected original %v (defensive copy on Put failed)", fetched1, originalKey)
	}

	// 2. Test defensive copying on Get
	fetched1[0] = 0xFF
	fetched2, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed on second call: %v", err)
	}
	if !bytes.Equal(fetched2, originalKey) {
		t.Errorf("Get returned %v, expected original %v (defensive copy on Get failed)", fetched2, originalKey)
	}

	// 3. Test explicit byte zeroing on Delete
	internalSlice := provider.keys[id]
	if !bytes.Equal(internalSlice, originalKey) {
		t.Fatalf("internalSlice does not match originalKey before deletion")
	}

	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key map entry is deleted
	if _, err := provider.Get(ctx, id); err == nil {
		t.Errorf("expected error when getting deleted key, got nil")
	}

	// Verify underlying byte slice was zeroed out
	for i, b := range internalSlice {
		if b != 0 {
			t.Errorf("expected byte at index %d to be zeroed, got 0x%02x", i, b)
		}
	}
}

func TestLocalKeyProvider_DeleteNonExistent(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()
	id := core.ContentID{9, 9, 9}

	if err := provider.Delete(ctx, id); err != nil {
		t.Errorf("Delete non-existent key failed: %v", err)
	}
}
