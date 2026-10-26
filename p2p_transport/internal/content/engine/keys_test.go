package engine

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_BasicOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keystore_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	provider, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	ctx := context.Background()

	var id core.ContentID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatalf("failed to generate random id: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Test Put
	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// Verify key file path and permissions
	keyPath := provider.KeyPath(id)
	fileInfo, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("key file does not exist at %s: %v", keyPath, err)
	}

	if mode := fileInfo.Mode().Perm(); mode != 0600 {
		t.Errorf("expected file mode 0600, got %o", mode)
	}

	keysDir := filepath.Dir(keyPath)
	dirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("keys dir does not exist at %s: %v", keysDir, err)
	}

	if mode := dirInfo.Mode().Perm(); mode != 0700 {
		t.Errorf("expected dir mode 0700, got %o", mode)
	}

	// Test Get
	gotKey, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}

	if string(gotKey) != string(key) {
		t.Errorf("retrieved key mismatch")
	}

	// Test Reload / Restart persistence
	newProvider, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to recreate FSKeyProvider: %v", err)
	}

	reloadedKey, err := newProvider.Get(ctx, id)
	if err != nil {
		t.Fatalf("failed to get reloaded key: %v", err)
	}

	if string(reloadedKey) != string(key) {
		t.Errorf("reloaded key mismatch")
	}

	// Test Delete
	if err := newProvider.Delete(ctx, id); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	if _, err := newProvider.Get(ctx, id); err == nil {
		t.Errorf("expected error after delete, got nil")
	}

	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Errorf("expected key file to be removed from disk")
	}
}

func TestMaskKey(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	masked := MaskKey(key)
	if masked == string(key) || len(masked) == 64 {
		t.Errorf("MaskKey did not redact raw key: %s", masked)
	}
	if len(masked) == 0 {
		t.Errorf("MaskKey returned empty string")
	}
}
