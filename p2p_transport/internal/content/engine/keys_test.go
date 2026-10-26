package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_PutGetDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var id core.ContentID
	rand.Read(id[:])

	secretKey := make([]byte, 32)
	rand.Read(secretKey)

	// Get before Put should return ErrKeyNotFound
	_, err = kp.Get(ctx, id)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// Put key
	if err := kp.Put(ctx, id, secretKey); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify key file path and permissions
	expectedPath := filepath.Join(tmpDir, "keys", fmt.Sprintf("%s.key", hex.EncodeToString(id[:])))
	fileInfo, err := os.Stat(expectedPath)
	if err != nil {
		t.Fatalf("expected key file to exist at %s: %v", expectedPath, err)
	}

	if fileInfo.Mode().Perm() != 0600 {
		t.Errorf("expected key file permissions 0600, got %o", fileInfo.Mode().Perm())
	}

	dirInfo, err := os.Stat(filepath.Join(tmpDir, "keys"))
	if err != nil {
		t.Fatalf("expected keys dir to exist: %v", err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Errorf("expected keys dir permissions 0700, got %o", dirInfo.Mode().Perm())
	}

	// Get key
	gotKey, err := kp.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(gotKey) != string(secretKey) {
		t.Fatalf("Get key mismatch: got %x, want %x", gotKey, secretKey)
	}

	// Delete key
	if err := kp.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get after Delete should return ErrKeyNotFound
	_, err = kp.Get(ctx, id)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound after Delete, got %v", err)
	}
}

func TestFSKeyProvider_PersistenceAcrossRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-persistence-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	var id core.ContentID
	rand.Read(id[:])
	secretKey := make([]byte, 32)
	rand.Read(secretKey)

	// Instance 1
	kp1 := NewFSKeyProvider(tmpDir)
	if err := kp1.Put(ctx, id, secretKey); err != nil {
		t.Fatalf("kp1 Put failed: %v", err)
	}

	// Instance 2 (simulating daemon restart)
	kp2 := NewFSKeyProvider(tmpDir)
	gotKey, err := kp2.Get(ctx, id)
	if err != nil {
		t.Fatalf("kp2 Get failed after restart: %v", err)
	}
	if string(gotKey) != string(secretKey) {
		t.Fatalf("kp2 key mismatch after restart: got %x, want %x", gotKey, secretKey)
	}
}

func TestFSKeyProvider_Concurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-concurrency-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 20

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var id core.ContentID
			id[0] = byte(idx)
			key := []byte(fmt.Sprintf("secret-key-%d", idx))

			if err := kp.Put(ctx, id, key); err != nil {
				t.Errorf("concurrent Put error: %v", err)
				return
			}

			retrieved, err := kp.Get(ctx, id)
			if err != nil {
				t.Errorf("concurrent Get error: %v", err)
				return
			}
			if string(retrieved) != string(key) {
				t.Errorf("concurrent key mismatch: got %s, want %s", retrieved, key)
			}
		}(i)
	}

	wg.Wait()
}
