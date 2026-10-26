package engine

import (
	"bytes"
	"context"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_DeleteZeroize(t *testing.T) {
	kp := NewLocalKeyProvider()
	ctx := context.Background()

	var id core.ContentID
	id[0] = 0x01
	key := []byte{0xde, 0xad, 0xbe, 0xef, 0x12, 0x34, 0x56, 0x78}

	if err := kp.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// Retrieve underlying reference from keys map to verify memory zeroization
	kp.mu.Lock()
	internalSlice := kp.keys[id]
	kp.mu.Unlock()

	if internalSlice == nil {
		t.Fatalf("expected internal slice to exist")
	}

	// Delete key
	if err := kp.Delete(ctx, id); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	// Verify internal slice bytes are zeroized
	expectedZeroes := make([]byte, len(internalSlice))
	if !bytes.Equal(internalSlice, expectedZeroes) {
		t.Errorf("expected deleted key slice to be zeroized, got %v", internalSlice)
	}

	// Verify Get returns error and key is deleted from map
	if _, err := kp.Get(ctx, id); err == nil {
		t.Errorf("expected error getting deleted key, got nil")
	}
}

func TestLocalKeyProvider_CloseZeroize(t *testing.T) {
	kp := NewLocalKeyProvider()
	ctx := context.Background()

	var id1, id2 core.ContentID
	id1[0] = 0x01
	id2[0] = 0x02

	key1 := []byte{0x11, 0x22, 0x33, 0x44}
	key2 := []byte{0x55, 0x66, 0x77, 0x88}

	kp.Put(ctx, id1, key1)
	kp.Put(ctx, id2, key2)

	kp.mu.Lock()
	slice1 := kp.keys[id1]
	slice2 := kp.keys[id2]
	kp.mu.Unlock()

	if err := kp.Close(); err != nil {
		t.Fatalf("failed to close key provider: %v", err)
	}

	// Verify slices are zeroized
	zeroes1 := make([]byte, len(slice1))
	zeroes2 := make([]byte, len(slice2))

	if !bytes.Equal(slice1, zeroes1) {
		t.Errorf("expected slice1 to be zeroized after Close, got %v", slice1)
	}
	if !bytes.Equal(slice2, zeroes2) {
		t.Errorf("expected slice2 to be zeroized after Close, got %v", slice2)
	}

	// Verify map is nil and Get returns error
	if _, err := kp.Get(ctx, id1); err == nil {
		t.Errorf("expected error getting key after Close, got nil")
	}
}

func TestLocalKeyProvider_PutOverwriteZeroize(t *testing.T) {
	kp := NewLocalKeyProvider()
	ctx := context.Background()

	var id core.ContentID
	id[0] = 0x01

	oldKey := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	newKey := []byte{0x11, 0x22, 0x33, 0x44}

	kp.Put(ctx, id, oldKey)

	kp.mu.Lock()
	oldSlice := kp.keys[id]
	kp.mu.Unlock()

	// Overwrite
	kp.Put(ctx, id, newKey)

	// Verify old slice is zeroized
	zeroes := make([]byte, len(oldSlice))
	if !bytes.Equal(oldSlice, zeroes) {
		t.Errorf("expected overwritten key slice to be zeroized, got %v", oldSlice)
	}
}
