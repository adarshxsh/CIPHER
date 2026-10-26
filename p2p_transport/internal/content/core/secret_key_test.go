package core_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/core"
)

func TestSecretKey_InitializationAndDestroy(t *testing.T) {
	initialData := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	key, err := core.NewSecretKey(initialData)
	if err != nil {
		t.Fatalf("failed to create secret key: %v", err)
	}

	if key.Len() != 32 {
		t.Errorf("expected length 32, got %d", key.Len())
	}

	rawBytes, err := key.Bytes()
	if err != nil {
		t.Fatalf("failed to get bytes: %v", err)
	}

	if !bytes.Equal(rawBytes, initialData) {
		t.Errorf("rawBytes %v != initialData %v", rawBytes, initialData)
	}

	// Call Destroy
	key.Destroy()

	if !key.IsDestroyed() {
		t.Errorf("expected key to be marked as destroyed")
	}

	if key.IsPinned() {
		t.Errorf("expected key to not be pinned after destruction")
	}

	if _, err := key.Bytes(); !errors.Is(err, core.ErrKeyDestroyed) {
		t.Errorf("expected ErrKeyDestroyed after Destroy(), got %v", err)
	}

	// Check that the underlying rawBytes slice was explicitly zeroed
	for i, b := range rawBytes {
		if b != 0 {
			t.Errorf("byte at index %d was not zeroed: %d", i, b)
		}
	}
}

func TestSecretKey_NewRandomSecretKey(t *testing.T) {
	key, err := core.NewRandomSecretKey(32)
	if err != nil {
		t.Fatalf("failed to create random key: %v", err)
	}
	defer key.Destroy()

	if key.Len() != 32 {
		t.Errorf("expected length 32, got %d", key.Len())
	}

	rawBytes, err := key.Bytes()
	if err != nil {
		t.Fatalf("failed to get bytes: %v", err)
	}

	// Verify not all bytes are 0
	allZero := true
	for _, b := range rawBytes {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Errorf("random key contains only zeroes")
	}
}

func TestSecretKey_Clone(t *testing.T) {
	key1, err := core.NewRandomSecretKey(32)
	if err != nil {
		t.Fatalf("failed to create random key: %v", err)
	}

	key2, err := key1.Clone()
	if err != nil {
		t.Fatalf("failed to clone key: %v", err)
	}

	bytes1, _ := key1.Bytes()
	bytes2, _ := key2.Bytes()

	if !bytes.Equal(bytes1, bytes2) {
		t.Errorf("cloned key bytes do not match original key bytes")
	}

	// Destroy original, clone should remain intact
	key1.Destroy()

	if _, err := key2.Bytes(); err != nil {
		t.Errorf("cloned key should remain valid after original is destroyed")
	}

	key2.Destroy()

	if _, err := key2.Bytes(); !errors.Is(err, core.ErrKeyDestroyed) {
		t.Errorf("cloned key should be destroyed after Destroy()")
	}
}

func TestSecretKey_DoubleDestroy(t *testing.T) {
	key, err := core.NewRandomSecretKey(32)
	if err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	key.Destroy()
	// Second destroy should be a no-op
	key.Destroy()

	if !key.IsDestroyed() {
		t.Errorf("expected key to be destroyed")
	}
}
