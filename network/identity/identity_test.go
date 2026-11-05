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
	tempDir := t.TempDir()
	t.Setenv("CIPHER_CONFIG_DIR", tempDir)

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

func TestEncryptedEnvelopeStructure(t *testing.T) {
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, "identity.key")

	priv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateFromPath failed: %v", err)
	}

	data, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("Failed to read key file: %v", err)
	}

	// Verify raw key bytes are not plaintext in the file
	rawBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to marshal raw private key: %v", err)
	}
	if bytes.Contains(data, rawBytes) {
		t.Fatalf("Key file contains unencrypted raw private key bytes!")
	}

	// Verify structure is a valid KeyEnvelope JSON
	var env KeyEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("Key file is not valid KeyEnvelope JSON: %v", err)
	}

	if env.Version != EnvelopeVersion {
		t.Errorf("Expected envelope version %d, got %d", EnvelopeVersion, env.Version)
	}

	if env.KDF != KDFArgon2id {
		t.Errorf("Expected KDF %s, got %s", KDFArgon2id, env.KDF)
	}

	if len(env.Salt) == 0 {
		t.Errorf("Expected non-empty salt in envelope")
	}

	if len(env.Nonce) == 0 {
		t.Errorf("Expected non-empty nonce in envelope")
	}

	if len(env.Ciphertext) == 0 {
		t.Errorf("Expected non-empty ciphertext in envelope")
	}
}

func TestPermissionEnforcement(t *testing.T) {
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, "identity.key")

	_, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateFromPath failed: %v", err)
	}

	// Deliberately set permissive mode 0644
	if err := os.Chmod(keyPath, 0644); err != nil {
		t.Fatalf("Failed to set permissive permissions: %v", err)
	}

	// Reloading should enforce 0600
	_, err = LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateFromPath failed on permissive file: %v", err)
	}

	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0600 {
		t.Fatalf("Expected file permissions 0600, got %04o", perm)
	}
}

func TestLegacyPlaintextMigration(t *testing.T) {
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, "legacy_identity.key")

	// Generate legacy plaintext Ed25519 key
	legacyPriv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate legacy key: %v", err)
	}

	legacyBytes, err := crypto.MarshalPrivateKey(legacyPriv)
	if err != nil {
		t.Fatalf("Failed to marshal legacy key: %v", err)
	}

	// Save legacy plaintext key with permissive mode 0644
	if err := os.WriteFile(keyPath, legacyBytes, 0644); err != nil {
		t.Fatalf("Failed to write legacy key file: %v", err)
	}

	// Load via LoadOrCreateFromPath (should migrate)
	migratedPriv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateFromPath failed during migration: %v", err)
	}

	if !migratedPriv.Equals(legacyPriv) {
		t.Fatalf("Migrated key does not match original legacy key")
	}

	// Verify file mode was restricted to 0600
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Fatalf("Expected file mode 0600 after migration, got %04o", perm)
	}

	// Verify file on disk is now an encrypted JSON envelope
	data, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("Failed to read migrated file: %v", err)
	}

	var env KeyEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("Migrated file is not valid KeyEnvelope JSON: %v", err)
	}

	if len(env.Ciphertext) == 0 {
		t.Fatalf("Migrated file ciphertext is empty")
	}

	// Verify reloading from migrated file works seamlessly
	reloadedPriv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to load migrated key on second run: %v", err)
	}

	if !reloadedPriv.Equals(legacyPriv) {
		t.Fatalf("Reloaded migrated key does not match original key")
	}
}

func TestPassphraseEnvironmentVariable(t *testing.T) {
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, "passphrase_identity.key")

	passphrase := "my-secret-vault-passphrase-123"
	t.Setenv("CIPHER_IDENTITY_PASSPHRASE", passphrase)

	priv, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to create key with passphrase: %v", err)
	}

	// Reload with same passphrase
	reloaded, err := LoadOrCreateFromPath(keyPath)
	if err != nil {
		t.Fatalf("Failed to reload key with same passphrase: %v", err)
	}
	if !reloaded.Equals(priv) {
		t.Fatalf("Reloaded key with passphrase does not match original")
	}

	// Reload with wrong passphrase should fail
	t.Setenv("CIPHER_IDENTITY_PASSPHRASE", "wrong-passphrase")
	_, err = LoadOrCreateFromPath(keyPath)
	if err == nil {
		t.Fatalf("Expected error when attempting to decrypt key with wrong passphrase, but got success")
	}
}

func TestCorruptedEnvelopeError(t *testing.T) {
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, "corrupted.key")

	if err := os.WriteFile(keyPath, []byte("invalid non-key non-json data"), 0600); err != nil {
		t.Fatalf("Failed to write corrupted file: %v", err)
	}

	_, err := LoadOrCreateFromPath(keyPath)
	if err == nil {
		t.Fatalf("Expected error when reading corrupted key file, got nil")
	}
}
