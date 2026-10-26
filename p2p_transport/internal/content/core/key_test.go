package core_test

import (
	"bytes"
	"runtime"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestZeroBytes(t *testing.T) {
	b := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0xAA, 0xBB, 0xCC}
	core.ZeroBytes(b)
	for i, v := range b {
		if v != 0 {
			t.Errorf("byte at index %d is non-zero: %d", i, v)
		}
	}
}

func TestKeyHandle_ExplicitClose(t *testing.T) {
	secret := []byte("super-secret-key-32-bytes-long!")
	kh := core.NewKeyHandle(secret)

	keyBytes := kh.Bytes()
	if !bytes.Equal(keyBytes, secret) {
		t.Fatalf("expected key bytes %v, got %v", secret, keyBytes)
	}
	if kh.IsClosed() {
		t.Fatal("expected handle to be open")
	}

	if err := kh.Close(); err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}

	if !kh.IsClosed() {
		t.Fatal("expected handle to be closed")
	}

	// Verify the slice referenced by keyBytes was zeroed in memory
	for i, b := range keyBytes {
		if b != 0 {
			t.Errorf("byte at index %d not zeroed after Close: got 0x%02x", i, b)
		}
	}

	if kh.Bytes() != nil {
		t.Fatal("Bytes() should return nil after Close")
	}

	// Calling Close again should be idempotent
	if err := kh.Close(); err != nil {
		t.Fatalf("subsequent Close returned error: %v", err)
	}
}

func TestKeyHandle_Finalizer(t *testing.T) {
	secret := []byte("finalizer-secret-key-1234567890!")
	
	// Allocate in helper function to allow GC collection
	var keyBytesRef []byte
	alloc := func() *core.KeyHandle {
		kh := core.NewKeyHandle(secret)
		keyBytesRef = kh.Bytes()
		return kh
	}

	kh := alloc()
	_ = kh // keep reference in local scope for now

	// Ensure bytes are non-zero before GC
	if bytes.Equal(keyBytesRef, make([]byte, len(secret))) {
		t.Fatal("key bytes unexpectedly zero prior to GC")
	}

	// Discard handle reference and trigger GC
	kh = nil
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	// Verify that finalizer ran and zeroed the underlying byte array
	for i, b := range keyBytesRef {
		if b != 0 {
			t.Errorf("byte at index %d not zeroed after finalizer GC: got 0x%02x", i, b)
		}
	}
}
