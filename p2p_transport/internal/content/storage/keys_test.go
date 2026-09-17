package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fskeyprovider-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	provider := NewFSKeyProvider(tmpDir)
	ctx := context.Background()

	var contentID core.ContentID
	if _, err := rand.Read(contentID[:]); err != nil {
		t.Fatalf("Failed to generate content ID: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	// 1. Put key
	if err := provider.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Check file location and permissions
	keyFilePath := filepath.Join(tmpDir, "keys", hex.EncodeToString(contentID[:])+".key")
	info, err := os.Stat(keyFilePath)
	if err != nil {
		t.Fatalf("Key file not created at expected path: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected permissions 0600, got %o", perm)
	}

	// 3. Get key
	gotKey, err := provider.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(key, gotKey) {
		t.Fatalf("Retrieved key does not match inserted key")
	}

	// 4. Persistence across restarts (new provider instance with same dir)
	provider2 := NewFSKeyProvider(tmpDir)
	restartedKey, err := provider2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get on restarted provider failed: %v", err)
	}
	if !bytes.Equal(key, restartedKey) {
		t.Fatalf("Restarted provider key does not match original key")
	}

	// 5. Delete key
	if err := provider.Delete(ctx, contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 6. Verify file removed
	if _, err := os.Stat(keyFilePath); !os.IsNotExist(err) {
		t.Fatalf("Expected key file to be removed after delete, but it exists")
	}

	// 7. Get after delete should return error
	if _, err := provider.Get(ctx, contentID); err == nil {
		t.Fatalf("Expected error getting deleted key, got nil")
	}
}

func TestKeyImportExport(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "keyexport-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	key := make([]byte, 32)
	rand.Read(key)

	// Binary export & import
	binPath := filepath.Join(tmpDir, "key.bin")
	if err := ExportKeyToFile(binPath, key); err != nil {
		t.Fatalf("ExportKeyToFile failed: %v", err)
	}
	info, err := os.Stat(binPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("Expected 0600 permissions on exported key file")
	}
	importedBin, err := LoadKeyFromFile(binPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile failed: %v", err)
	}
	if !bytes.Equal(key, importedBin) {
		t.Fatalf("Imported binary key mismatch")
	}

	// Hex export & import
	hexPath := filepath.Join(tmpDir, "key.hex")
	hexStr := hex.EncodeToString(key)
	os.WriteFile(hexPath, []byte(hexStr+"\n"), 0600)

	importedHex, err := LoadKeyFromFile(hexPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile hex failed: %v", err)
	}
	if !bytes.Equal(key, importedHex) {
		t.Fatalf("Imported hex key mismatch")
	}
}
