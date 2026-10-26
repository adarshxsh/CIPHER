package identity

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/crypto/argon2"
)

const (
	EnvelopeVersion = 1
	KDFArgon2id     = "argon2id"

	defaultArgon2Time    uint32 = 1
	defaultArgon2Memory  uint32 = 64 * 1024 // 64 MB
	defaultArgon2Threads uint8  = 2
	defaultArgon2KeyLen  uint32 = 32 // 256 bits for AES-256
)

// KeyEnvelope defines the JSON envelope structure stored on disk.
type KeyEnvelope struct {
	Version    int        `json:"version"`
	KDF        string     `json:"kdf"`
	KDFParams  *KDFParams `json:"kdf_params,omitempty"`
	Salt       []byte     `json:"salt"`
	Nonce      []byte     `json:"nonce"`
	Ciphertext []byte     `json:"ciphertext"`
}

// KDFParams defines parameters for key derivation using Argon2id.
type KDFParams struct {
	Time    uint32 `json:"time"`
	Memory  uint32 `json:"memory"`
	Threads uint8  `json:"threads"`
	KeyLen  uint32 `json:"key_len"`
}

// LoadOrCreate loads an existing Ed25519 private key from the user's config directory,
// or generates a new one and saves it if it doesn't exist.
func LoadOrCreate() (crypto.PrivKey, error) {
	configDir := os.Getenv("CIPHER_CONFIG_DIR")
	if configDir == "" {
		var err error
		configDir, err = os.UserConfigDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get user config dir: %w", err)
		}
	}

	appDir := filepath.Join(configDir, "cipher")
	keyPath := filepath.Join(appDir, "identity.key")
	return LoadOrCreateFromPath(keyPath)
}

// LoadOrCreateFromPath loads an existing Ed25519 key from keyPath or generates and saves one.
func LoadOrCreateFromPath(keyPath string) (crypto.PrivKey, error) {
	passphrase := getPassphraseOrMachineID()

	// Try to load existing key
	if _, err := os.Stat(keyPath); err == nil {
		if err := enforceMode0600(keyPath); err != nil {
			return nil, err
		}

		data, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read key file: %w", err)
		}

		// Try decrypting as encrypted envelope
		priv, err := decryptEnvelope(data, passphrase)
		if err == nil {
			log.Printf("[identity] Successfully loaded encrypted key vault envelope from %s", keyPath)
			return priv, nil
		}

		// If decrypting envelope failed, check if it's a legacy plaintext key
		legacyPriv, legacyErr := crypto.UnmarshalPrivateKey(data)
		if legacyErr == nil {
			log.Printf("[identity] Detected legacy plaintext identity key at %s, migrating to encrypted key vault envelope...", keyPath)
			envBytes, encErr := encryptKey(legacyPriv, passphrase)
			if encErr != nil {
				return nil, fmt.Errorf("failed to encrypt legacy key during migration: %w", encErr)
			}

			if err := writeEnvelopeAtomically(keyPath, envBytes); err != nil {
				return nil, fmt.Errorf("failed to save migrated key vault envelope: %w", err)
			}

			log.Printf("[identity] Successfully migrated legacy identity key at %s to encrypted key vault envelope", keyPath)
			return legacyPriv, nil
		}

		return nil, fmt.Errorf("failed to load identity key: %w", err)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to stat key file: %w", err)
	}

	// Generate a new Ed25519 key
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	envBytes, err := encryptKey(priv, passphrase)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt key: %w", err)
	}

	if err := writeEnvelopeAtomically(keyPath, envBytes); err != nil {
		return nil, fmt.Errorf("failed to save encrypted key: %w", err)
	}

	log.Printf("[identity] Successfully initialized new encrypted key vault envelope at %s", keyPath)
	return priv, nil
}

// GenerateEphemeral generates a one-time in-memory Ed25519 private key without persisting to disk.
func GenerateEphemeral() (crypto.PrivKey, error) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ephemeral key pair: %w", err)
	}
	return priv, nil
}

func getPassphraseOrMachineID() []byte {
	envVars := []string{
		"CIPHER_IDENTITY_PASSPHRASE",
		"CIPHER_PASSPHRASE",
		"IDENTITY_PASSPHRASE",
		"PASSPHRASE",
	}
	for _, envVar := range envVars {
		if val := os.Getenv(envVar); val != "" {
			return []byte(val)
		}
	}

	// Try reading Linux machine-id
	machineIDFiles := []string{
		"/etc/machine-id",
		"/var/lib/dbus/machine-id",
	}
	for _, fPath := range machineIDFiles {
		data, err := os.ReadFile(fPath)
		if err == nil && len(data) > 0 {
			return data
		}
	}

	// Fallback: hostname + user
	hostname, _ := os.Hostname()
	u, _ := user.Current()
	var username string
	if u != nil {
		username = u.Username
	}
	fallback := fmt.Sprintf("cipher-machine-id:%s:%s", hostname, username)
	return []byte(fallback)
}

func deriveKey(passphrase []byte, salt []byte, params *KDFParams) []byte {
	if params == nil {
		params = &KDFParams{
			Time:    defaultArgon2Time,
			Memory:  defaultArgon2Memory,
			Threads: defaultArgon2Threads,
			KeyLen:  defaultArgon2KeyLen,
		}
	}
	return argon2.IDKey(passphrase, salt, params.Time, params.Memory, params.Threads, params.KeyLen)
}

func encryptKey(priv crypto.PrivKey, passphrase []byte) ([]byte, error) {
	keyBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	salt := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	params := &KDFParams{
		Time:    defaultArgon2Time,
		Memory:  defaultArgon2Memory,
		Threads: defaultArgon2Threads,
		KeyLen:  defaultArgon2KeyLen,
	}

	derivedKey := deriveKey(passphrase, salt, params)

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create AEAD: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, keyBytes, nil)

	env := &KeyEnvelope{
		Version:    EnvelopeVersion,
		KDF:        KDFArgon2id,
		KDFParams:  params,
		Salt:       salt,
		Nonce:      nonce,
		Ciphertext: ciphertext,
	}

	envBytes, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key envelope JSON: %w", err)
	}

	return envBytes, nil
}

func decryptEnvelope(envBytes []byte, passphrase []byte) (crypto.PrivKey, error) {
	var env KeyEnvelope
	if err := json.Unmarshal(envBytes, &env); err != nil {
		return nil, fmt.Errorf("invalid envelope json: %w", err)
	}

	if len(env.Ciphertext) == 0 || len(env.Salt) == 0 || len(env.Nonce) == 0 {
		return nil, fmt.Errorf("incomplete key envelope structure")
	}

	derivedKey := deriveKey(passphrase, env.Salt, env.KDFParams)

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create AEAD: %w", err)
	}

	plaintext, err := gcm.Open(nil, env.Nonce, env.Ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt identity envelope (invalid passphrase or corrupted key): %w", err)
	}

	priv, err := crypto.UnmarshalPrivateKey(plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	return priv, nil
}

func enforceMode0600(keyPath string) error {
	info, err := os.Stat(keyPath)
	if err != nil {
		return err
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		if err := os.Chmod(keyPath, 0600); err != nil {
			return fmt.Errorf("key file %s has insecure permissions %04o and chmod 0600 failed: %w", keyPath, perm, err)
		}
		log.Printf("[identity] Restricted permissive key file permissions for %s from %04o to 0600", keyPath, perm)
	}
	return nil
}

func writeEnvelopeAtomically(keyPath string, envBytes []byte) error {
	dir := filepath.Dir(keyPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tmpFile := keyPath + ".tmp"
	if err := os.WriteFile(tmpFile, envBytes, 0600); err != nil {
		return fmt.Errorf("failed to write temp key envelope: %w", err)
	}

	if err := os.Rename(tmpFile, keyPath); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to atomically save key envelope: %w", err)
	}

	return enforceMode0600(keyPath)
}
