package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_LifecycleAndPermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	// Verify keys directory permissions (0700)
	keysDir := filepath.Join(tmpDir, "keys")
	info, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("failed to stat keys dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("keys dir permission = %o; want 0700", perm)
	}

	ctx := context.Background()
	var contentID core.ContentID
	if _, err := rand.Read(contentID[:]); err != nil {
		t.Fatalf("failed to generate random ContentID: %v", err)
	}

	testKey := make([]byte, 32)
	if _, err := rand.Read(testKey); err != nil {
		t.Fatalf("failed to generate random key: %v", err)
	}

	// 1. Test Put
	if err := kp.Put(ctx, contentID, testKey); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify key file existence and POSIX 0600 permissions
	keyFilePath := filepath.Join(keysDir, hex.EncodeToString(contentID[:])+".key")
	keyInfo, err := os.Stat(keyFilePath)
	if err != nil {
		t.Fatalf("failed to stat key file: %v", err)
	}
	if perm := keyInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("key file permission = %o; want 0600", perm)
	}

	// 2. Test Get
	gotKey, err := kp.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(gotKey, testKey) {
		t.Errorf("Get returned %x, want %x", gotKey, testKey)
	}

	// 3. Test Disk Pre-loading upon Restart
	kp2, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second FSKeyProvider instance: %v", err)
	}

	loadedKey, err := kp2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get on reloaded provider failed: %v", err)
	}
	if !bytes.Equal(loadedKey, testKey) {
		t.Errorf("loaded key = %x, want %x", loadedKey, testKey)
	}

	// 4. Test Delete
	if err := kp2.Delete(ctx, contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if _, err := kp2.Get(ctx, contentID); err == nil {
		t.Errorf("Get after Delete should fail, but succeeded")
	}

	if _, err := os.Stat(keyFilePath); !os.IsNotExist(err) {
		t.Errorf("key file should be deleted from disk, but stat returned: %v", err)
	}
}

func TestFSKeyProvider_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-concurrent-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	numRoutines := 20

	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			var id core.ContentID
			id[0] = byte(idx)

			key := make([]byte, 32)
			key[0] = byte(idx)

			if err := kp.Put(ctx, id, key); err != nil {
				t.Errorf("Put failed in goroutine %d: %v", idx, err)
				return
			}

			readKey, err := kp.Get(ctx, id)
			if err != nil {
				t.Errorf("Get failed in goroutine %d: %v", idx, err)
				return
			}

			if !bytes.Equal(readKey, key) {
				t.Errorf("goroutine %d got key %x, want %x", idx, readKey, key)
			}
		}(i)
	}

	wg.Wait()
}

func TestKeyHelpers(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "key-helpers-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	rawKey := make([]byte, 32)
	rand.Read(rawKey)

	// Test KeyID fingerprint
	fingerprint := KeyID(rawKey)
	if len(fingerprint) != 16 {
		t.Errorf("KeyID length = %d; want 16 (hex representation of 8 bytes)", len(fingerprint))
	}

	// Test ExportKeyToFile and LoadKeyFromFile (Hex format)
	exportPath := filepath.Join(tmpDir, "exported.key")
	if err := ExportKeyToFile(exportPath, rawKey); err != nil {
		t.Fatalf("ExportKeyToFile failed: %v", err)
	}

	// Verify export file permissions (0600)
	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatalf("failed to stat export file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("export file permission = %o; want 0600", perm)
	}

	loadedKey, err := LoadKeyFromFile(exportPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile failed: %v", err)
	}
	if !bytes.Equal(loadedKey, rawKey) {
		t.Errorf("loaded key = %x; want %x", loadedKey, rawKey)
	}

	// Test LoadKeyFromFile with raw binary 32-byte format
	rawPath := filepath.Join(tmpDir, "raw.key")
	if err := os.WriteFile(rawPath, rawKey, 0600); err != nil {
		t.Fatalf("failed to write raw key file: %v", err)
	}

	loadedRawKey, err := LoadKeyFromFile(rawPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile raw failed: %v", err)
	}
	if !bytes.Equal(loadedRawKey, rawKey) {
		t.Errorf("loaded raw key = %x; want %x", loadedRawKey, rawKey)
	}
}
