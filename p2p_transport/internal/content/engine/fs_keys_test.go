package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_PutGetDelete(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fs_keys_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	kp, err := NewFSKeyProvider(tempDir)
	if err != nil {
		t.Fatalf("NewFSKeyProvider failed: %v", err)
	}

	ctx := context.Background()
	var contentID core.ContentID
	if _, err := rand.Read(contentID[:]); err != nil {
		t.Fatalf("Failed to generate content ID: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	// Put key
	if err := kp.Put(ctx, contentID, key); err != nil {
		t.Fatalf("FSKeyProvider.Put failed: %v", err)
	}

	// Verify key file exists and permissions are 0600
	expectedPath := filepath.Join(tempDir, "keys", hex.EncodeToString(contentID[:])+".key")
	info, err := os.Stat(expectedPath)
	if err != nil {
		t.Fatalf("Key file not created: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("Expected permissions 0600, got %o", info.Mode().Perm())
	}

	// Get key
	retrievedKey, err := kp.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("FSKeyProvider.Get failed: %v", err)
	}
	if string(retrievedKey) != string(key) {
		t.Errorf("Retrieved key does not match original key")
	}

	// Test persistence by reloading
	kp2, err := NewFSKeyProvider(tempDir)
	if err != nil {
		t.Fatalf("NewFSKeyProvider reload failed: %v", err)
	}
	reloadedKey, err := kp2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Reloaded key get failed: %v", err)
	}
	if string(reloadedKey) != string(key) {
		t.Errorf("Reloaded key does not match original key")
	}

	// Delete key
	if err := kp.Delete(ctx, contentID); err != nil {
		t.Fatalf("FSKeyProvider.Delete failed: %v", err)
	}
	if _, err := kp.Get(ctx, contentID); err == nil {
		t.Errorf("Expected error after delete, got nil")
	}
	if _, err := os.Stat(expectedPath); !os.IsNotExist(err) {
		t.Errorf("Expected key file to be removed from disk")
	}
}

func TestLoadAndExportKeyToFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "export_keys_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	key := make([]byte, 32)
	rand.Read(key)

	exportPath := filepath.Join(tempDir, "sub", "test.key")
	if err := ExportKeyToFile(exportPath, key); err != nil {
		t.Fatalf("ExportKeyToFile failed: %v", err)
	}

	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatalf("Key file stat failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("Expected permissions 0600 on exported key file, got %o", info.Mode().Perm())
	}

	// Load key back
	loadedKey, err := LoadKeyFromFile(exportPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile failed: %v", err)
	}
	if string(loadedKey) != string(key) {
		t.Errorf("Loaded key does not match exported key")
	}

	// Test raw binary loading
	rawPath := filepath.Join(tempDir, "raw.key")
	if err := os.WriteFile(rawPath, key, 0600); err != nil {
		t.Fatalf("Write raw key file failed: %v", err)
	}
	loadedRawKey, err := LoadKeyFromFile(rawPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile raw failed: %v", err)
	}
	if string(loadedRawKey) != string(key) {
		t.Errorf("Loaded raw key does not match original key")
	}

	// Test invalid key file length
	invalidPath := filepath.Join(tempDir, "invalid.key")
	os.WriteFile(invalidPath, []byte("shortkey"), 0600)
	if _, err := LoadKeyFromFile(invalidPath); err == nil {
		t.Errorf("Expected error loading invalid key file, got nil")
	}
}

func TestKeyID(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	keyID := KeyID(key)
	if len(keyID) != 16 { // 8 bytes hex encoded = 16 characters
		t.Errorf("Expected KeyID length 16, got %d", len(keyID))
	}

	if strings.Contains(keyID, hex.EncodeToString(key)) {
		t.Errorf("KeyID should not leak raw hex key")
	}

	if KeyID(nil) != "[none]" {
		t.Errorf("Expected KeyID(nil) to be [none]")
	}
}
