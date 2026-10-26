package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-key-provider-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	provider := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))
	sampleKey := []byte("secretkey01234567890123456789012") // 32 bytes

	// 1. Put key
	if err := provider.Put(ctx, contentID, sampleKey); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	// 2. Verify key store directory permissions (0700)
	keysDir := filepath.Join(tmpDir, "keys")
	dirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("failed to stat keys directory: %v", err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Errorf("expected keys dir perm 0700, got %o", dirInfo.Mode().Perm())
	}

	// 3. Verify key file path and permissions (0600)
	keyFilePath := filepath.Join(keysDir, hex.EncodeToString(contentID[:])+".key")
	fileInfo, err := os.Stat(keyFilePath)
	if err != nil {
		t.Fatalf("failed to stat key file: %v", err)
	}
	if fileInfo.Mode().Perm() != 0600 {
		t.Errorf("expected key file perm 0600, got %o", fileInfo.Mode().Perm())
	}

	// 4. Get key from in-memory cache
	retrievedKey, err := provider.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("failed to get key from provider: %v", err)
	}
	if !bytes.Equal(sampleKey, retrievedKey) {
		t.Errorf("retrieved key mismatch")
	}

	// 5. Test persistence across restarts (instantiate new provider pointing to same storeDir)
	restartProvider := NewFSKeyProvider(tmpDir)
	restartedKey, err := restartProvider.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("failed to retrieve key after process restart: %v", err)
	}
	if !bytes.Equal(sampleKey, restartedKey) {
		t.Errorf("key mismatch after restart")
	}

	// 6. Delete key
	if err := restartProvider.Delete(ctx, contentID); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}
	if _, err := os.Stat(keyFilePath); !os.IsNotExist(err) {
		t.Errorf("expected key file to be deleted from disk")
	}
	if _, err := restartProvider.Get(ctx, contentID); err == nil {
		t.Errorf("expected Get to fail after Delete")
	}
}
