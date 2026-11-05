package keyutil

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"cipher/network/content/core"
)

func TestExportAndLoadKey_Binary(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test.key")

	rawKey := make([]byte, 32)
	if _, err := rand.Read(rawKey); err != nil {
		t.Fatalf("failed to generate random key: %v", err)
	}

	if err := ExportKey(keyPath, rawKey); err != nil {
		t.Fatalf("ExportKey failed: %v", err)
	}

	// Verify permissions
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected file mode 0600, got %o", perm)
	}

	// Load via keyutil
	loaded, err := LoadKey(keyPath)
	if err != nil {
		t.Fatalf("LoadKey failed: %v", err)
	}

	if !bytes.Equal(rawKey, loaded) {
		t.Fatalf("loaded key does not match original key")
	}
}

func TestLoadKey_HexFile(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test_hex.key")

	rawKey := make([]byte, 32)
	rand.Read(rawKey)
	hexStr := hex.EncodeToString(rawKey)

	if err := os.WriteFile(keyPath, []byte(hexStr+"\n"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	loaded, err := LoadKey(keyPath)
	if err != nil {
		t.Fatalf("LoadKey failed: %v", err)
	}

	if !bytes.Equal(rawKey, loaded) {
		t.Fatalf("loaded key does not match original key")
	}
}

func TestLoadKey_InlineHex(t *testing.T) {
	rawKey := make([]byte, 32)
	rand.Read(rawKey)
	hexStr := hex.EncodeToString(rawKey)

	loaded, err := LoadKey(hexStr)
	if err != nil {
		t.Fatalf("LoadKey failed: %v", err)
	}

	if !bytes.Equal(rawKey, loaded) {
		t.Fatalf("loaded key does not match original key")
	}
}

func TestDefaultKeyPath(t *testing.T) {
	var cID core.ContentID
	copy(cID[:], []byte("12345678901234567890123456789012"))

	expected := filepath.Join("/store", fmt.Sprintf("%x.key", cID))
	actual := DefaultKeyPath("/store", cID)
	if actual != expected {
		t.Fatalf("expected %s, got %s", expected, actual)
	}
}
