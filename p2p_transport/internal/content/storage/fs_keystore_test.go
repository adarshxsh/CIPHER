package storage

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyStore_PersistenceAndPermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keystore_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	ks, err := NewFSKeyStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FSKeyStore: %v", err)
	}

	var contentID core.ContentID
	if _, err := rand.Read(contentID[:]); err != nil {
		t.Fatalf("Failed to generate content id: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate random key: %v", err)
	}

	// 1. Test Put
	if err := ks.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Verify Get
	gotKey, err := ks.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Fatalf("Retrieved key does not match inserted key")
	}

	// 3. Verify directory POSIX permissions (0700)
	keysDir := filepath.Join(tmpDir, "keys")
	dirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("Failed to stat keys dir: %v", err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Errorf("Expected keys dir permissions 0700, got %o", dirInfo.Mode().Perm())
	}

	// 4. Verify file POSIX permissions (0600)
	entries, err := os.ReadDir(keysDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("Failed to read keys dir entries: %v", err)
	}

	var keyFileFound bool
	for _, entry := range entries {
		if entry.Name() == ".masterkey" {
			continue
		}
		keyFileFound = true
		keyPath := filepath.Join(keysDir, entry.Name())
		fileInfo, err := os.Stat(keyPath)
		if err != nil {
			t.Fatalf("Failed to stat key file: %v", err)
		}
		if fileInfo.Mode().Perm() != 0600 {
			t.Errorf("Expected key file permissions 0600, got %o", fileInfo.Mode().Perm())
		}
		// Verify file content is encrypted (size != 32 bytes)
		if fileInfo.Size() == 32 {
			t.Errorf("Key file on disk is cleartext (32 bytes), expected encrypted payload")
		}
	}
	if !keyFileFound {
		t.Fatalf("Key file not found in keys directory")
	}

	// 5. Test persistence across process restart simulation
	ks2, err := NewFSKeyStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to recreate FSKeyStore: %v", err)
	}
	reloadedKey, err := ks2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get after restart failed: %v", err)
	}
	if string(reloadedKey) != string(key) {
		t.Fatalf("Reloaded key after restart does not match original key")
	}

	// 6. Test Delete
	if err := ks2.Delete(ctx, contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = ks2.Get(ctx, contentID)
	if err == nil {
		t.Fatalf("Expected error when getting deleted key, got nil")
	}
}
