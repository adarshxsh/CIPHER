package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_PutGetDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	provider := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var id core.ContentID
	rand.Read(id[:])

	key := make([]byte, 32)
	rand.Read(key)

	// 1. Get non-existent key
	_, err = provider.Get(ctx, id)
	if err == nil {
		t.Fatal("expected error getting non-existent key, got nil")
	}

	// 2. Put key
	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// 3. Get key (from cache)
	gotKey, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Fatalf("key mismatch: got %x, want %x", gotKey, key)
	}

	// 4. Delete key
	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	// 5. Get deleted key
	_, err = provider.Get(ctx, id)
	if err == nil {
		t.Fatal("expected error getting deleted key, got nil")
	}
}

func TestFSKeyProvider_Permissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-perm-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	provider := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var id core.ContentID
	rand.Read(id[:])
	key := make([]byte, 32)
	rand.Read(key)

	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// Verify directory permissions 0700
	keysDir := filepath.Join(tmpDir, "keys")
	dirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("failed to stat keys dir: %v", err)
	}
	dirPerm := dirInfo.Mode().Perm()
	if dirPerm != 0700 {
		t.Errorf("expected directory permission 0700, got %#o", dirPerm)
	}

	// Verify key file permissions 0600
	keyPath := filepath.Join(keysDir, hex.EncodeToString(id[:])+".key")
	fileInfo, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("failed to stat key file: %v", err)
	}
	filePerm := fileInfo.Mode().Perm()
	if filePerm != 0600 {
		t.Errorf("expected key file permission 0600, got %#o", filePerm)
	}
}

func TestFSKeyProvider_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-persist-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	provider1 := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var id core.ContentID
	rand.Read(id[:])
	key := make([]byte, 32)
	rand.Read(key)

	if err := provider1.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// Create a new provider instance pointing to the same store directory
	provider2 := NewFSKeyProvider(tmpDir)

	// Fetch key from provider2 (should load from disk)
	gotKey, err := provider2.Get(ctx, id)
	if err != nil {
		t.Fatalf("provider2 failed to get key from disk: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Fatalf("persistence key mismatch: got %x, want %x", gotKey, key)
	}
}
