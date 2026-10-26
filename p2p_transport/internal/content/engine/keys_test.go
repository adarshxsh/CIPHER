package engine

import (
	"context"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_DeleteZeroesMemory(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{1, 2, 3}
	key := []byte("secret-key-123456789012345678901234")

	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to Put key: %v", err)
	}

	// Capture pointer/slice to the internal stored key byte array before deletion
	storedKeySlice := provider.keys[id]
	if len(storedKeySlice) == 0 {
		t.Fatal("expected non-empty stored key slice")
	}

	// Delete key
	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("failed to Delete key: %v", err)
	}

	// Verify key was removed from map
	if _, err := provider.Get(ctx, id); err == nil {
		t.Fatal("expected Get after Delete to fail")
	}

	// Verify the stored key memory was explicitly overwritten with zero bytes
	for i, b := range storedKeySlice {
		if b != 0 {
			t.Errorf("byte at index %d was not zeroed after Delete: got 0x%02x", i, b)
		}
	}
}

func TestLocalKeyProvider_GetAndClose(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{4, 5, 6}
	key := []byte("my-super-secret-encryption-key32")

	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to Put key: %v", err)
	}

	keyHandle, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("failed to Get key handle: %v", err)
	}

	keyBytes := keyHandle.Bytes()
	if len(keyBytes) != len(key) {
		t.Fatalf("expected key length %d, got %d", len(key), len(keyBytes))
	}

	if err := keyHandle.Close(); err != nil {
		t.Fatalf("failed to Close key handle: %v", err)
	}

	// Verify keyBytes memory is zeroed out
	for i, b := range keyBytes {
		if b != 0 {
			t.Errorf("byte at index %d was not zeroed after KeyHandle.Close: got 0x%02x", i, b)
		}
	}
}

func TestLocalKeyProvider_PutZeroesOldKey(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{7, 8, 9}
	key1 := []byte("first-secret-key-123456789012345")
	key2 := []byte("second-secret-key-12345678901234")

	if err := provider.Put(ctx, id, key1); err != nil {
		t.Fatalf("failed to Put key1: %v", err)
	}

	oldKeySlice := provider.keys[id]

	// Overwrite with key2
	if err := provider.Put(ctx, id, key2); err != nil {
		t.Fatalf("failed to Put key2: %v", err)
	}

	// Verify old key memory was zeroed
	for i, b := range oldKeySlice {
		if b != 0 {
			t.Errorf("byte at index %d of old key was not zeroed on Put overwrite: got 0x%02x", i, b)
		}
	}
}

func TestLocalKeyProvider_CloseZeroesAllKeys(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id1 := core.ContentID{10}
	id2 := core.ContentID{11}

	_ = provider.Put(ctx, id1, []byte("key1-12345678901234567890123456789012"))
	_ = provider.Put(ctx, id2, []byte("key2-12345678901234567890123456789012"))

	k1 := provider.keys[id1]
	k2 := provider.keys[id2]

	if err := provider.Close(); err != nil {
		t.Fatalf("failed to Close provider: %v", err)
	}

	for i, b := range k1 {
		if b != 0 {
			t.Errorf("k1 byte %d not zeroed: 0x%02x", i, b)
		}
	}
	for i, b := range k2 {
		if b != 0 {
			t.Errorf("k2 byte %d not zeroed: 0x%02x", i, b)
		}
	}
	if len(provider.keys) != 0 {
		t.Errorf("expected empty keys map after Close, got size %d", len(provider.keys))
	}
}
