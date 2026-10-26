package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_PutGetDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keys-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	keysDir := filepath.Join(tmpDir, "keys")
	kp, err := NewFSKeyProvider(keysDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	ctx := context.Background()
	var contentID core.ContentID
	rand.Read(contentID[:])

	key := make([]byte, 32)
	rand.Read(key)

	// Put
	if err := kp.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	gotKey, err := kp.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("Get key mismatch: got %x, want %x", gotKey, key)
	}

	// Delete
	if err := kp.Delete(ctx, contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get after Delete should fail
	_, err = kp.Get(ctx, contentID)
	if err == nil {
		t.Fatal("expected Get after Delete to fail, but succeeded")
	}
}

func TestFSKeyProvider_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keys-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	keysDir := filepath.Join(tmpDir, "keys")
	ctx := context.Background()

	var contentID core.ContentID
	rand.Read(contentID[:])
	key := make([]byte, 32)
	rand.Read(key)

	// Instance 1 writes key
	kp1, err := NewFSKeyProvider(keysDir)
	if err != nil {
		t.Fatalf("failed to create kp1: %v", err)
	}
	if err := kp1.Put(ctx, contentID, key); err != nil {
		t.Fatalf("kp1.Put failed: %v", err)
	}

	// Instance 2 (simulating process restart) reads key
	kp2, err := NewFSKeyProvider(keysDir)
	if err != nil {
		t.Fatalf("failed to create kp2: %v", err)
	}
	gotKey, err := kp2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("kp2.Get failed: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("kp2 key mismatch: got %x, want %x", gotKey, key)
	}
}

func TestFSKeyProvider_Permissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keys-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	keysDir := filepath.Join(tmpDir, "keys")
	kp, err := NewFSKeyProvider(keysDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	ctx := context.Background()
	var contentID core.ContentID
	rand.Read(contentID[:])
	key := make([]byte, 32)
	rand.Read(key)

	if err := kp.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	keyFilePath := kp.keyPath(contentID)
	info, err := os.Stat(keyFilePath)
	if err != nil {
		t.Fatalf("failed to stat key file: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Fatalf("expected file permission 0600, got %o", perm)
	}
}

func TestFSKeyProvider_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keys-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	keysDir := filepath.Join(tmpDir, "keys")
	kp, err := NewFSKeyProvider(keysDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	var contentID core.ContentID
	rand.Read(contentID[:])

	_, err = kp.Get(context.Background(), contentID)
	if err == nil {
		t.Fatal("expected error for non-existent key, got nil")
	}
}
