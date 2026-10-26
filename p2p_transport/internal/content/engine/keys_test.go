package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func TestEncryptedFileKeyProvider_BasicOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keystore-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "subDir", "keystore.db")
	provider, err := NewEncryptedFileKeyProvider(dbPath)
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer provider.Close()

	ctx := context.Background()
	var cid core.ContentID
	rand.Read(cid[:])

	secretKey := []byte("01234567890123456789012345678901") // 32 bytes

	// Get non-existent key
	_, err = provider.Get(ctx, cid)
	if err == nil {
		t.Fatalf("expected error getting non-existent key")
	}

	// Put key
	if err := provider.Put(ctx, cid, secretKey); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// Get key
	retrieved, err := provider.Get(ctx, cid)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if !bytes.Equal(retrieved, secretKey) {
		t.Fatalf("got key %x != expected %x", retrieved, secretKey)
	}

	// Delete key
	if err := provider.Delete(ctx, cid); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	// Get key again
	_, err = provider.Get(ctx, cid)
	if err == nil {
		t.Fatalf("expected error getting deleted key")
	}
}

func TestEncryptedFileKeyProvider_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keystore-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "keystore.db")
	masterKey := []byte("master-secret-key-32-bytes-long!")

	ctx := context.Background()
	var cid1, cid2 core.ContentID
	rand.Read(cid1[:])
	rand.Read(cid2[:])

	key1 := []byte("key-1-012345678901234567890123456")
	key2 := []byte("key-2-012345678901234567890123456")

	// Phase 1: Write keys with process 1 instance
	p1, err := NewEncryptedFileKeyProvider(dbPath, masterKey)
	if err != nil {
		t.Fatalf("failed to create p1: %v", err)
	}
	if err := p1.Put(ctx, cid1, key1); err != nil {
		t.Fatalf("failed to put key1: %v", err)
	}
	if err := p1.Put(ctx, cid2, key2); err != nil {
		t.Fatalf("failed to put key2: %v", err)
	}
	_ = p1.Close()

	// Phase 2: Read keys with process 2 instance (simulating restart)
	p2, err := NewEncryptedFileKeyProvider(dbPath, masterKey)
	if err != nil {
		t.Fatalf("failed to create p2 on restart: %v", err)
	}
	defer p2.Close()

	got1, err := p2.Get(ctx, cid1)
	if err != nil || !bytes.Equal(got1, key1) {
		t.Fatalf("key1 recovery failed across process restart: got %v, err %v", got1, err)
	}

	got2, err := p2.Get(ctx, cid2)
	if err != nil || !bytes.Equal(got2, key2) {
		t.Fatalf("key2 recovery failed across process restart: got %v, err %v", got2, err)
	}
}

func TestFileKeyProviderAlias(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keystore-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "keystore.db")
	p, err := NewFileKeyProvider(dbPath)
	if err != nil {
		t.Fatalf("failed to create FileKeyProvider: %v", err)
	}
	defer p.Close()

	ctx := context.Background()
	var cid core.ContentID
	rand.Read(cid[:])
	key := []byte("01234567890123456789012345678901")

	if err := p.Put(ctx, cid, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}
	got, err := p.Get(ctx, cid)
	if err != nil || !bytes.Equal(got, key) {
		t.Fatalf("failed to get key via FileKeyProvider")
	}
}

func TestEncryptedFileKeyProvider_FilePermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keystore-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "cipher", "keystore.db")
	provider, err := NewEncryptedFileKeyProvider(dbPath)
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer provider.Close()

	ctx := context.Background()
	var cid core.ContentID
	rand.Read(cid[:])
	key := []byte("secret-key-permission-test-32b!!")

	if err := provider.Put(ctx, cid, key); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// Verify directory permissions 0700
	dirInfo, err := os.Stat(filepath.Dir(dbPath))
	if err != nil {
		t.Fatalf("failed to stat directory: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("dir permissions %o != expected 0700", perm)
	}

	// Verify db file permissions 0600
	fileInfo, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("failed to stat db file: %v", err)
	}
	if perm := fileInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("db file permissions %o != expected 0600", perm)
	}

	// Verify master key file permissions 0600
	masterKeyPath := filepath.Join(filepath.Dir(dbPath), "keystore.key")
	mKeyInfo, err := os.Stat(masterKeyPath)
	if err != nil {
		t.Fatalf("failed to stat master key file: %v", err)
	}
	if perm := mKeyInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("master key file permissions %o != expected 0600", perm)
	}
}

func TestDefaultKeystorePath(t *testing.T) {
	path := DefaultKeystorePath()
	if path == "" {
		t.Fatalf("DefaultKeystorePath returned empty string")
	}
	if filepath.Base(path) != "keystore.db" {
		t.Errorf("expected keystore.db base name, got %s", filepath.Base(path))
	}
}

func TestEncryptedFileKeyProvider_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keystore-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "keystore.db")
	provider, err := NewEncryptedFileKeyProvider(dbPath)
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer provider.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var cid core.ContentID
			cid[0] = byte(idx)

			key := []byte("concurrent-key-data-32-bytes!!!")
			_ = provider.Put(ctx, cid, key)
			ret, err := provider.Get(ctx, cid)
			if err == nil && !bytes.Equal(ret, key) {
				t.Errorf("goroutine %d got invalid key bytes", idx)
			}
		}(i)
	}

	wg.Wait()
}
