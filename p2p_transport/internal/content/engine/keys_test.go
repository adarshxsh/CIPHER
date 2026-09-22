package engine

import (
	"context"
	"testing"

	"cipher/internal/content/core"
)

func TestWipe(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x23, 0x45, 0x67}
	Wipe(data)
	for i, b := range data {
		if b != 0 {
			t.Fatalf("at index %d, expected byte 0, got 0x%02x", i, b)
		}
	}
}

func TestKeyHandle_Release(t *testing.T) {
	secret := []byte("topsecretkey12345678901234567890")
	handle := NewKeyHandle(secret)

	keyBytes := handle.Bytes()
	if len(keyBytes) != len(secret) {
		t.Fatalf("expected handle byte length %d, got %d", len(secret), len(keyBytes))
	}

	handle.Release()

	// Verify that handle.Bytes() returns nil after release
	if handle.Bytes() != nil {
		t.Fatalf("expected nil bytes after handle release")
	}

	// Verify underlying buffer was wiped
	for i, b := range keyBytes {
		if b != 0 {
			t.Fatalf("at index %d, expected wiped byte 0, got 0x%02x", i, b)
		}
	}
}

func TestLocalKeyProvider_Delete_WipesMemory(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{1, 2, 3}
	secret := []byte("supersecretkey12345678901234567")

	if err := provider.Put(ctx, id, secret); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Obtain reference to stored map byte slice before deletion
	provider.mu.RLock()
	storedSlice := provider.keys[id]
	provider.mu.RUnlock()

	if storedSlice == nil {
		t.Fatalf("expected stored slice to be non-nil")
	}

	// Delete from key provider
	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key is gone from provider
	_, err := provider.Get(ctx, id)
	if err == nil {
		t.Fatalf("expected error getting deleted key, got nil")
	}

	// Verify the slice referenced by storedSlice was wiped with zeros
	for i, b := range storedSlice {
		if b != 0 {
			t.Fatalf("at index %d of deleted key memory, expected 0, got 0x%02x", i, b)
		}
	}
}

func TestLocalKeyProvider_GetHandle_And_Overwrite(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{10, 20}
	secret1 := []byte("firstsecretkey123456789012345678")
	secret2 := []byte("secondsecretkey1234567890123456")

	if err := provider.Put(ctx, id, secret1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	handle1, err := provider.GetHandle(ctx, id)
	if err != nil {
		t.Fatalf("GetHandle failed: %v", err)
	}
	defer handle1.Release()

	if string(handle1.Bytes()) != string(secret1) {
		t.Fatalf("expected %s, got %s", secret1, handle1.Bytes())
	}

	// Overwrite key
	provider.mu.RLock()
	oldSlice := provider.keys[id]
	provider.mu.RUnlock()

	if err := provider.Put(ctx, id, secret2); err != nil {
		t.Fatalf("Put overwrite failed: %v", err)
	}

	// Old slice should be wiped
	for i, b := range oldSlice {
		if b != 0 {
			t.Fatalf("at index %d of overwritten key memory, expected 0, got 0x%02x", i, b)
		}
	}
}
