package storage

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
	tempDir, err := os.MkdirTemp("", "fskeyprovider_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	kp := NewFSKeyProvider(tempDir)
	ctx := context.Background()

	var id core.ContentID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatalf("Failed to generate content ID: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate random key: %v", err)
	}

	// 1. Put key
	if err := kp.Put(ctx, id, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Get key
	gotKey, err := kp.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(key, gotKey) {
		t.Fatalf("Key mismatch. Expected %x, got %x", key, gotKey)
	}

	// 3. Delete key
	if err := kp.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 4. Get after delete should fail
	_, err = kp.Get(ctx, id)
	if err == nil {
		t.Fatalf("Expected error getting deleted key, got nil")
	}
}

func TestFSKeyProvider_Persistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fskeyprovider_pers_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	kp1 := NewFSKeyProvider(tempDir)
	ctx := context.Background()

	var id core.ContentID
	rand.Read(id[:])
	key := make([]byte, 32)
	rand.Read(key)

	if err := kp1.Put(ctx, id, key); err != nil {
		t.Fatalf("kp1 Put failed: %v", err)
	}

	// Simulate process restart by creating new FSKeyProvider with same store path
	kp2 := NewFSKeyProvider(tempDir)
	gotKey, err := kp2.Get(ctx, id)
	if err != nil {
		t.Fatalf("kp2 Get after restart failed: %v", err)
	}
	if !bytes.Equal(key, gotKey) {
		t.Fatalf("Persisted key mismatch. Expected %x, got %x", key, gotKey)
	}
}

func TestFSKeyProvider_Permissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fskeyprovider_perm_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	kp := NewFSKeyProvider(tempDir)
	ctx := context.Background()

	var id core.ContentID
	rand.Read(id[:])
	key := make([]byte, 32)
	rand.Read(key)

	if err := kp.Put(ctx, id, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	keysDir := filepath.Join(tempDir, "keys")
	keysDirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("Failed to stat keys directory: %v", err)
	}

	expectedDirMode := os.FileMode(0700)
	if keysDirInfo.Mode().Perm() != expectedDirMode {
		t.Errorf("Keys directory mode mismatch. Expected %o, got %o", expectedDirMode, keysDirInfo.Mode().Perm())
	}

	keyPath := kp.keyPath(id)
	keyFileInfo, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Failed to stat key file: %v", err)
	}

	expectedFileMode := os.FileMode(0600)
	if keyFileInfo.Mode().Perm() != expectedFileMode {
		t.Errorf("Key file mode mismatch. Expected %o, got %o", expectedFileMode, keyFileInfo.Mode().Perm())
	}
}
