package engine

import (
	"context"
	"bytes"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyHandle_ZeroizeOnRelease(t *testing.T) {
	key := []byte("secretkey01234567890123456789012")
	handle := core.NewKeyHandle(key)

	b := handle.Bytes()
	if !bytes.Equal(b, key) {
		t.Fatalf("expected handle bytes %q, got %q", key, b)
	}

	handle.Release()

	if handle.Bytes() != nil {
		t.Errorf("expected handle.Bytes() to be nil after Release()")
	}

	for i, v := range b {
		if v != 0 {
			t.Errorf("expected byte at index %d to be 0 after Release(), got %d", i, v)
		}
	}

	// Idempotency check on Release
	handle.Release()
}

func TestLocalKeyProvider_ZeroizeOnDelete(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	var id core.ContentID
	copy(id[:], []byte("contentid01234567890123456789012"))

	key := []byte("supersecretkey012345678901234567")
	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to Put key: %v", err)
	}

	handle, err := provider.GetHandle(ctx, id)
	if err != nil {
		t.Fatalf("failed to GetHandle: %v", err)
	}

	b := handle.Bytes()
	if !bytes.Equal(b, key) {
		t.Fatalf("expected handle bytes to match stored key")
	}

	// Delete from provider
	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("failed to Delete key: %v", err)
	}

	// GetHandle should now fail
	if _, err := provider.GetHandle(ctx, id); err == nil {
		t.Errorf("expected error getting deleted key handle")
	}

	// Releasing handle should zeroize handle buffer
	handle.Release()

	for i, v := range b {
		if v != 0 {
			t.Errorf("expected handle byte at index %d to be zeroed out, got %d", i, v)
		}
	}
}

func TestLocalKeyProvider_ZeroizeOnPutOverwrite(t *testing.T) {
	ctx := context.Background()
	provider := NewLocalKeyProvider()

	var id core.ContentID
	copy(id[:], []byte("contentid01234567890123456789012"))

	key1 := []byte("firstkey012345678901234567890123")
	if err := provider.Put(ctx, id, key1); err != nil {
		t.Fatalf("failed to Put key1: %v", err)
	}

	// Overwrite with key2
	key2 := []byte("secondkey01234567890123456789012")
	if err := provider.Put(ctx, id, key2); err != nil {
		t.Fatalf("failed to Put key2: %v", err)
	}

	handle2, err := provider.GetHandle(ctx, id)
	if err != nil {
		t.Fatalf("failed to GetHandle for key2: %v", err)
	}
	defer handle2.Release()

	if !bytes.Equal(handle2.Bytes(), key2) {
		t.Errorf("expected stored key to match key2")
	}
}
