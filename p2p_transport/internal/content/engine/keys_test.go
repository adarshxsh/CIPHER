package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_PutGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	provider := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var id core.ContentID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatalf("failed to generate random id: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// 1. Get before Put should return ErrKeyNotFound
	_, err := provider.Get(ctx, id)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// 2. Put key
	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 3. Verify directory permissions (0700) and file permissions (0600)
	keysDir := filepath.Join(tmpDir, "keys")
	dirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("failed to stat keys dir: %v", err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Errorf("expected keys dir permission 0700, got %o", dirInfo.Mode().Perm())
	}

	keyPath := provider.keyPath(id)
	fileInfo, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("failed to stat key file: %v", err)
	}
	if fileInfo.Mode().Perm() != 0600 {
		t.Errorf("expected key file permission 0600, got %o", fileInfo.Mode().Perm())
	}

	// 4. Get key
	retrievedKey, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(key, retrievedKey) {
		t.Errorf("retrieved key does not match original key")
	}

	// 5. Delete key
	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 6. Get after Delete should return ErrKeyNotFound
	_, err = provider.Get(ctx, id)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound after Delete, got %v", err)
	}
}

func TestFSKeyProvider_PersistenceAcrossInstances(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	var id core.ContentID
	rand.Read(id[:])
	key := make([]byte, 32)
	rand.Read(key)

	provider1 := NewFSKeyProvider(tmpDir)
	if err := provider1.Put(ctx, id, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Create a new provider instance pointing to the same store
	provider2 := NewFSKeyProvider(tmpDir)
	retrievedKey, err := provider2.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get on second provider instance failed: %v", err)
	}
	if !bytes.Equal(key, retrievedKey) {
		t.Errorf("key from second provider instance does not match original")
	}
}
