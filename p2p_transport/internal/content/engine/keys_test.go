package engine

import (
	"context"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_DeleteZeroization(t *testing.T) {
	kp := NewLocalKeyProvider()
	ctx := context.Background()

	var id core.ContentID
	copy(id[:], []byte("test-content-id-1234567890123456"))

	secret := []byte("super-secret-key-32-bytes-long!")
	if err := kp.Put(ctx, id, secret); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Keep a slice reference to the stored internal byte array
	kp.mu.RLock()
	storedKeyRef := kp.keys[id]
	kp.mu.RUnlock()

	if len(storedKeyRef) == 0 {
		t.Fatalf("expected non-empty stored key")
	}

	// Delete key
	if err := kp.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key map entry was removed
	if _, err := kp.Get(ctx, id); err == nil {
		t.Errorf("expected error after Delete, got nil")
	}

	// Verify internal byte slice memory was explicitly zeroed
	for i, b := range storedKeyRef {
		if b != 0 {
			t.Errorf("byte at index %d was not zeroed: %d", i, b)
		}
	}
}

func TestLocalKeyProvider_GetHandleReleaseZeroization(t *testing.T) {
	kp := NewLocalKeyProvider()
	ctx := context.Background()

	var id core.ContentID
	copy(id[:], []byte("test-content-id-1234567890123456"))

	secret := []byte("super-secret-key-32-bytes-long!")
	if err := kp.Put(ctx, id, secret); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	handle, err := kp.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	callerKey := handle.Bytes()
	if len(callerKey) != len(secret) {
		t.Fatalf("expected key length %d, got %d", len(secret), len(callerKey))
	}

	// Verify handle release wipes caller key copy
	handle.Release()

	for i, b := range callerKey {
		if b != 0 {
			t.Errorf("caller key byte at index %d was not zeroed: %d", i, b)
		}
	}

	if handle.Bytes() != nil {
		t.Errorf("expected handle.Bytes() to return nil after release")
	}
}

func TestLocalKeyProvider_PutOverwriteZeroization(t *testing.T) {
	kp := NewLocalKeyProvider()
	ctx := context.Background()

	var id core.ContentID
	copy(id[:], []byte("test-content-id-1234567890123456"))

	secret1 := []byte("first-secret-key-32-bytes-long!!")
	if err := kp.Put(ctx, id, secret1); err != nil {
		t.Fatalf("Put 1 failed: %v", err)
	}

	kp.mu.RLock()
	oldKeyRef := kp.keys[id]
	kp.mu.RUnlock()

	secret2 := []byte("second-secret-key-32-bytes-long!")
	if err := kp.Put(ctx, id, secret2); err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	// Verify old key memory was zeroed
	for i, b := range oldKeyRef {
		if b != 0 {
			t.Errorf("overwritten key byte at index %d was not zeroed: %d", i, b)
		}
	}
}
