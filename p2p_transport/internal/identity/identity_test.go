package identity

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestLoadOrCreate(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("CIPHER_CONFIG_DIR", tmpDir)

	priv1, err := LoadOrCreate()
	if err != nil {
		t.Fatalf("Failed to create first key: %v", err)
	}

	priv2, err := LoadOrCreate()
	if err != nil {
		t.Fatalf("Failed to load second key: %v", err)
	}

	if !priv1.Equals(priv2) {
		t.Fatalf("Expected keys to be equal on reload")
	}
}

func TestEncryptedKeyFormatAndPermissions(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test_identity.key")

	priv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to load or create key: %v", err)
	}

	// Check file mode 0600
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Failed to stat key file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("Expected file permissions 0600, got %o", info.Mode().Perm())
	}

	// Read file contents
	data, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("Failed to read key file: %v", err)
	}

	// Verify JSON structure
	var env KeyEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("Expected file to be valid JSON KeyEnvelope: %v", err)
	}

	if env.Algorithm != "AES-256-GCM" {
		t.Fatalf("Expected algorithm AES-256-GCM, got %s", env.Algorithm)
	}
	if len(env.Salt) != 16 {
		t.Fatalf("Expected 16-byte salt, got %d bytes", len(env.Salt))
	}
	if len(env.Nonce) != 12 {
		t.Fatalf("Expected 12-byte nonce, got %d bytes", len(env.Nonce))
	}
	if len(env.Ciphertext) == 0 {
		t.Fatalf("Expected non-empty ciphertext")
	}

	// Verify raw key bytes are not stored in plaintext
	rawBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to marshal private key: %v", err)
	}

	if bytes.Contains(data, rawBytes) {
		t.Fatalf("Key file contains raw plaintext private key bytes!")
	}
}

func TestPassphraseConfiguration(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "passphrase_identity.key")

	t.Setenv("CIPHER_IDENTITY_PASSPHRASE", "correct-horse-battery-staple")

	priv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to create key with passphrase: %v", err)
	}

	// Attempt reload with wrong passphrase
	t.Setenv("CIPHER_IDENTITY_PASSPHRASE", "wrong-passphrase")
	_, err = LoadOrCreateFromPath(keyPath)
	if err == nil {
		t.Fatalf("Expected decryption failure with wrong passphrase, but load succeeded")
	}

	// Reload with correct passphrase
	t.Setenv("CIPHER_IDENTITY_PASSPHRASE", "correct-horse-battery-staple")
	reloadedPriv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to reload key with correct passphrase: %v", err)
	}

	if !priv.Equals(reloadedPriv) {
		t.Fatalf("Reloaded key does not match original key")
	}
}

func TestInsecurePermissionsCorrection(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "insecure_permissions.key")

	priv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to create key: %v", err)
	}

	// Set permissions to world-readable (0644)
	if err := os.Chmod(keyPath, 0644); err != nil {
		t.Fatalf("Failed to chmod key file to 0644: %v", err)
	}

	// LoadOrCreateFromPath should fix permissions to 0600
	reloadedPriv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to load key with fixed permissions: %v", err)
	}

	if !priv.Equals(reloadedPriv) {
		t.Fatalf("Reloaded key does not match original key")
	}

	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Failed to stat key file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("Expected permissions to be corrected to 0600, got %o", info.Mode().Perm())
	}
}

func TestLegacyPlaintextMigration(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "legacy_identity.key")

	// Generate legacy plaintext private key
	legacyPriv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	legacyBytes, err := crypto.MarshalPrivateKey(legacyPriv)
	if err != nil {
		t.Fatalf("Failed to marshal private key: %v", err)
	}

	// Write plaintext bytes directly to key file with 0644 permissions
	if err := os.WriteFile(keyPath, legacyBytes, 0644); err != nil {
		t.Fatalf("Failed to write legacy key file: %v", err)
	}

	// Load key from path (should trigger auto-migration and permission fix)
	loadedPriv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to load and migrate legacy key: %v", err)
	}

	if !legacyPriv.Equals(loadedPriv) {
		t.Fatalf("Migrated key does not match legacy private key")
	}

	// Verify file is now an encrypted JSON KeyEnvelope with 0600 permissions
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Failed to stat migrated key file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("Expected file permission to be migrated to 0600, got %o", info.Mode().Perm())
	}

	data, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("Failed to read migrated key file: %v", err)
	}

	var env KeyEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("Migrated file is not a valid JSON KeyEnvelope: %v", err)
	}

	if env.Algorithm != "AES-256-GCM" {
		t.Fatalf("Expected algorithm AES-256-GCM, got %s", env.Algorithm)
	}

	if bytes.Contains(data, legacyBytes) {
		t.Fatalf("Migrated file still contains raw plaintext private key bytes!")
	}
}

func TestGenerateEphemeral(t *testing.T) {
	priv1, err := GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate ephemeral key 1: %v", err)
	}

	priv2, err := GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate ephemeral key 2: %v", err)
	}

	if priv1.Equals(priv2) {
		t.Fatalf("Ephemeral keys should be unique")
	}
}
