package engine

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_FilePersistence(t *testing.T) {
	tempDir := t.TempDir()
	provider := NewLocalKeyProvider(tempDir)

	var id core.ContentID
	copy(id[:], []byte("12345678901234567890123456789012"))
	secretKey := []byte("0123456789abcdef0123456789abcdef")

	ctx := context.Background()

	// 1. Test Put saves to disk with 0600 permissions
	if err := provider.Put(ctx, id, secretKey); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	keyPath := filepath.Join(tempDir, "keys", hex.EncodeToString(id[:])+".key")
	stat, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Expected key file at %s, got error: %v", keyPath, err)
	}

	perm := stat.Mode().Perm()
	if perm != 0600 {
		t.Errorf("Expected file permission 0600, got %o", perm)
	}

	// 2. Test Get loads from disk if in-memory cache is empty
	newProvider := NewLocalKeyProvider(tempDir)
	retrievedKey, err := newProvider.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed to load key from disk: %v", err)
	}
	if string(retrievedKey) != string(secretKey) {
		t.Errorf("Key mismatch: expected %x, got %x", secretKey, retrievedKey)
	}

	// 3. Test LoadKeyFromFile with hex format
	hexKeyFile := filepath.Join(tempDir, "hex.key")
	hexString := hex.EncodeToString(secretKey)
	if err := os.WriteFile(hexKeyFile, []byte(hexString+"\n"), 0600); err != nil {
		t.Fatalf("Failed to write hex key file: %v", err)
	}

	loadedFromHex, err := LoadKeyFromFile(hexKeyFile)
	if err != nil {
		t.Fatalf("LoadKeyFromFile failed for hex file: %v", err)
	}
	if string(loadedFromHex) != string(secretKey) {
		t.Errorf("Hex key mismatch: expected %x, got %x", secretKey, loadedFromHex)
	}

	// 4. Test Delete removes file from disk
	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Errorf("Expected key file to be deleted, but it still exists")
	}
}
