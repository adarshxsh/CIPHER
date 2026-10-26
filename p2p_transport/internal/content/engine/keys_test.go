package engine

import (
	"context"
	"testing"

	"cipher/internal/content/core"
)

func TestProtectedBuffer_ZeroingOnDestroy(t *testing.T) {
	keyData := []byte("secret-key-32-bytes-long-for-test!")
	buf := NewProtectedBuffer(keyData)

	ref := buf.Bytes()
	if string(ref) != string(keyData) {
		t.Fatalf("expected buffer content %q, got %q", keyData, ref)
	}

	if buf.IsClosed() {
		t.Fatalf("expected buffer to be active")
	}

	buf.Destroy()

	if !buf.IsClosed() {
		t.Fatalf("expected buffer to be closed after Destroy")
	}

	if !buf.IsZero() {
		t.Fatalf("expected IsZero to be true after Destroy")
	}

	// Verify underlying byte slice was zeroed out
	for i, b := range ref {
		if b != 0 {
			t.Errorf("byte at index %d is %d, expected 0", i, b)
		}
	}

	if buf.Bytes() != nil {
		t.Errorf("expected Bytes() to return nil when closed")
	}
}

func TestLocalKeyProvider_PutGetDelete_Zeroing(t *testing.T) {
	kp := NewLocalKeyProvider()
	defer kp.Close()

	ctx := context.Background()
	var contentID core.ContentID
	copy(contentID[:], []byte("test-content-id-1234567890123456"))

	secretKey := []byte("very-secret-encryption-key-data!")

	if err := kp.Put(ctx, contentID, secretKey); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Retrieve reference inside WithKey
	var keySliceCopy []byte
	err := kp.WithKey(ctx, contentID, func(key []byte) error {
		if string(key) != string(secretKey) {
			t.Errorf("WithKey provided wrong key: %q", key)
		}
		// Grab a slice sharing underlying array of the protected buffer to test zeroing on Delete
		pb := kp.keys[contentID]
		keySliceCopy = pb.Bytes()
		return nil
	})
	if err != nil {
		t.Fatalf("WithKey failed: %v", err)
	}

	// Now Delete the key
	if err := kp.Delete(ctx, contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key cannot be retrieved anymore
	err = kp.WithKey(ctx, contentID, func(key []byte) error {
		return nil
	})
	if err == nil {
		t.Fatalf("expected error accessing deleted key")
	}

	// Verify that keySliceCopy (referencing the destroyed ProtectedBuffer) contains only zero bytes
	for i, b := range keySliceCopy {
		if b != 0 {
			t.Errorf("deleted key byte at index %d is %d, expected 0", i, b)
		}
	}
}

func TestLocalKeyProvider_WithKey_PanicSafety(t *testing.T) {
	kp := NewLocalKeyProvider()
	defer kp.Close()

	ctx := context.Background()
	var contentID core.ContentID
	contentID[0] = 0xAA

	secretKey := []byte("secret-key-panic-test-1234567890")
	_ = kp.Put(ctx, contentID, secretKey)

	var capturedKey []byte
	func() {
		defer func() {
			_ = recover()
		}()

		_ = kp.WithKey(ctx, contentID, func(key []byte) error {
			capturedKey = key
			panic("simulated panic inside WithKey")
		})
	}()

	// Verify the temporary key copy passed to callback was wiped even on panic
	for i, b := range capturedKey {
		if b != 0 {
			t.Errorf("temporary key byte at index %d is %d after panic, expected 0", i, b)
		}
	}
}

func TestLocalKeyProvider_Overwrite_ZeroesPreviousBuffer(t *testing.T) {
	kp := NewLocalKeyProvider()
	defer kp.Close()

	ctx := context.Background()
	var contentID core.ContentID
	contentID[0] = 0xBB

	key1 := []byte("first-secret-key-123456789012345")
	_ = kp.Put(ctx, contentID, key1)

	oldBuf := kp.keys[contentID]
	ref1 := oldBuf.Bytes()

	key2 := []byte("second-secret-key-12345678901234")
	_ = kp.Put(ctx, contentID, key2)

	// Verify old buffer was destroyed and zeroed
	if !oldBuf.IsClosed() {
		t.Errorf("expected old buffer to be closed after Put overwrite")
	}
	for i, b := range ref1 {
		if b != 0 {
			t.Errorf("overwritten key byte at index %d is %d, expected 0", i, b)
		}
	}

	// Verify new key is stored
	_ = kp.WithKey(ctx, contentID, func(k []byte) error {
		if string(k) != string(key2) {
			t.Errorf("expected new key %q, got %q", key2, k)
		}
		return nil
	})
}
