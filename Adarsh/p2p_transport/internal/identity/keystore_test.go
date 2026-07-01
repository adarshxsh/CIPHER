package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrGenerateKey(t *testing.T) {
	// Create a temporary directory for the tests
	tmpDir, err := os.MkdirTemp("", "keystore_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	keyPath := filepath.Join(tmpDir, "identity.key")

	// 1. Generate a new key (file shouldn't exist initially)
	privKey1, err := LoadOrGenerateKey(keyPath)
	if err != nil {
		t.Fatalf("Failed to generate new key: %v", err)
	}
	if privKey1 == nil {
		t.Fatal("Expected private key, got nil")
	}

	// 2. Ensure file exists
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		t.Fatal("Key file was not created")
	}

	// 3. Load the existing key
	privKey2, err := LoadOrGenerateKey(keyPath)
	if err != nil {
		t.Fatalf("Failed to load existing key: %v", err)
	}
	if privKey2 == nil {
		t.Fatal("Expected private key, got nil")
	}

	// 4. Compare the two keys to ensure they are the same
	if !privKey1.Equals(privKey2) {
		t.Fatal("Loaded key does not match generated key")
	}
}
