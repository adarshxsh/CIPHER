package identity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestLoadOrCreate(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("CIPHER_CONFIG_DIR", tempDir)

	priv1, err := LoadOrCreate("test-passphrase")
	if err != nil {
		t.Fatalf("Failed to create first key: %v", err)
	}

	priv2, err := LoadOrCreate("test-passphrase")
	if err != nil {
		t.Fatalf("Failed to load second key: %v", err)
	}

	if !priv1.Equals(priv2) {
		t.Fatalf("Expected keys to be equal on reload")
	}

	// Ensure the encrypted file identity.key.enc exists
	encPath := filepath.Join(tempDir, "cipher", "identity.key.enc")
	if _, err := os.Stat(encPath); err != nil {
		t.Fatalf("Expected encrypted key file to exist at %s: %v", encPath, err)
	}
}

func TestEncryptedKeyContainer(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	passphrase := "secret-passphrase-123"
	envelope, err := EncryptKey(priv, passphrase)
	if err != nil {
		t.Fatalf("EncryptKey failed: %v", err)
	}

	if envelope.Version != 1 {
		t.Errorf("Expected version 1, got %d", envelope.Version)
	}
	if envelope.KDF != "scrypt" {
		t.Errorf("Expected KDF scrypt, got %s", envelope.KDF)
	}
	if envelope.Cipher != "aes-256-gcm" {
		t.Errorf("Expected Cipher aes-256-gcm, got %s", envelope.Cipher)
	}
	if envelope.ScryptParams.N != 32768 || envelope.ScryptParams.R != 8 || envelope.ScryptParams.P != 1 {
		t.Errorf("Unexpected scrypt parameters: %+v", envelope.ScryptParams)
	}
	if len(envelope.Nonce) == 0 {
		t.Error("Nonce should not be empty")
	}
	if len(envelope.Ciphertext) == 0 {
		t.Error("Ciphertext should not be empty")
	}
	if len(envelope.AuthTag) == 0 {
		t.Error("AuthTag should not be empty")
	}

	// Test successful decryption
	decryptedPriv, err := DecryptKey(envelope, passphrase)
	if err != nil {
		t.Fatalf("DecryptKey failed: %v", err)
	}
	if !priv.Equals(decryptedPriv) {
		t.Fatal("Decrypted key does not match original key")
	}

	// Test decryption failure with wrong passphrase
	_, err = DecryptKey(envelope, "wrong-passphrase")
	if err == nil {
		t.Fatal("Expected error when decrypting with wrong passphrase, got nil")
	}
}

func TestPassphraseResolution(t *testing.T) {
	// 1. Explicit parameter
	p1, err := ResolvePassphrase("explicit-pass")
	if err != nil || p1 != "explicit-pass" {
		t.Fatalf("Expected explicit-pass, got %s (err: %v)", p1, err)
	}

	// 2. Environment variable
	t.Setenv("CIPHER_IDENTITY_PASSPHRASE", "env-passphrase")
	p2, err := ResolvePassphrase()
	if err != nil || p2 != "env-passphrase" {
		t.Fatalf("Expected env-passphrase, got %s (err: %v)", p2, err)
	}

	// 3. Fallback when env and parameter are unset
	t.Setenv("CIPHER_IDENTITY_PASSPHRASE", "")
	p3, err := ResolvePassphrase()
	if err != nil || p3 == "" {
		t.Fatalf("Expected non-empty fallback passphrase, got %s (err: %v)", p3, err)
	}
}

func TestPermissionEnforcement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file permissions check skipped on Windows")
	}

	tempDir := t.TempDir()
	// Set valid directory permissions 0700
	if err := os.Chmod(tempDir, 0700); err != nil {
		t.Fatalf("Failed to set tempDir permissions: %v", err)
	}

	keyPath := filepath.Join(tempDir, "identity.key.enc")

	// Create valid encrypted key file
	priv, err := LoadOrCreateFromPath(keyPath, "test-pass")
	if err != nil {
		t.Fatalf("Failed to create key file: %v", err)
	}
	if priv == nil {
		t.Fatal("Expected non-nil private key")
	}

	// Test 1: Insecure file permissions (0644)
	if err := os.Chmod(keyPath, 0644); err != nil {
		t.Fatalf("Failed to set loose file permissions: %v", err)
	}
	_, err = LoadOrCreateFromPath(keyPath, "test-pass")
	if err == nil || !strings.Contains(err.Error(), "insecure key file permissions") {
		t.Fatalf("Expected insecure key file permissions error, got: %v", err)
	}

	// Test 2: Insecure file permissions (0777)
	if err := os.Chmod(keyPath, 0777); err != nil {
		t.Fatalf("Failed to set loose file permissions: %v", err)
	}
	_, err = LoadOrCreateFromPath(keyPath, "test-pass")
	if err == nil || !strings.Contains(err.Error(), "insecure key file permissions") {
		t.Fatalf("Expected insecure key file permissions error, got: %v", err)
	}

	// Fix file permissions back to 0600
	if err := os.Chmod(keyPath, 0600); err != nil {
		t.Fatalf("Failed to fix file permissions: %v", err)
	}

	// Test 3: Insecure parent directory permissions (0755)
	if err := os.Chmod(tempDir, 0755); err != nil {
		t.Fatalf("Failed to set loose dir permissions: %v", err)
	}
	_, err = LoadOrCreateFromPath(keyPath, "test-pass")
	if err == nil || !strings.Contains(err.Error(), "insecure parent directory permissions") {
		t.Fatalf("Expected insecure parent directory permissions error, got: %v", err)
	}
}

func TestLegacyPlaintextMigration(t *testing.T) {
	tempDir := t.TempDir()
	if runtime.GOOS != "windows" {
		_ = os.Chmod(tempDir, 0700)
	}

	legacyPath := filepath.Join(tempDir, "identity.key")
	encPath := filepath.Join(tempDir, "identity.key.enc")

	// Generate a legacy unencrypted Ed25519 key
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	legacyBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to marshal key: %v", err)
	}

	if err := os.WriteFile(legacyPath, legacyBytes, 0600); err != nil {
		t.Fatalf("Failed to write legacy key file: %v", err)
	}
	if runtime.GOOS != "windows" {
		_ = os.Chmod(legacyPath, 0600)
	}

	// Load via LoadOrCreateFromPath - should trigger migration
	passphrase := "migration-passphrase"
	loadedPriv, err := LoadOrCreateFromPath(legacyPath, passphrase)
	if err != nil {
		t.Fatalf("LoadOrCreateFromPath failed during legacy migration: %v", err)
	}

	if !priv.Equals(loadedPriv) {
		t.Fatal("Migrated key does not match original legacy key")
	}

	// Verify legacy file was removed
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("Expected legacy key file to be removed, but it still exists")
	}

	// Verify new encrypted container file exists
	if _, err := os.Stat(encPath); err != nil {
		t.Fatalf("Expected encrypted key file to exist at %s: %v", encPath, err)
	}

	// Verify encrypted container file is valid JSON and decryptable
	encBytes, err := os.ReadFile(encPath)
	if err != nil {
		t.Fatalf("Failed to read encrypted key file: %v", err)
	}

	var envelope EncryptedKeyEnvelope
	if err := json.Unmarshal(encBytes, &envelope); err != nil {
		t.Fatalf("Encrypted key file is not valid JSON: %v", err)
	}

	reloadedPriv, err := DecryptKey(&envelope, passphrase)
	if err != nil {
		t.Fatalf("Failed to decrypt migrated key envelope: %v", err)
	}

	if !priv.Equals(reloadedPriv) {
		t.Fatal("Reloaded decrypted key does not match original key")
	}
}

func TestGenerateEphemeral(t *testing.T) {
	priv, err := GenerateEphemeral()
	if err != nil {
		t.Fatalf("GenerateEphemeral failed: %v", err)
	}
	if priv == nil {
		t.Fatal("Expected non-nil private key")
	}
}
