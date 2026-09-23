package identity

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/crypto/pbkdf2"
)

// ErrInsecureFilePermissions is returned when a key file has insecure file permissions
// that cannot be corrected to mode 0600.
var ErrInsecureFilePermissions = errors.New("insecure file permissions")

const (
	pbkdf2Iterations = 100000
	pbkdf2KeyLen     = 32
	nonceSize        = 12
	saltSize         = 16
	algorithmName    = "AES-256-GCM"
)

// KeyEnvelope represents the JSON structure for persisted encrypted keys.
type KeyEnvelope struct {
	Algorithm  string `json:"algorithm"`
	Salt       []byte `json:"salt"`
	Nonce      []byte `json:"nonce"`
	Ciphertext []byte `json:"ciphertext"`
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
	// Check if file exists
	info, err := os.Stat(keyPath)
	if err == nil {
		// File permission check must run before reading key bytes on disk.
		if info.Mode().Perm() != 0600 {
			if chmodErr := os.Chmod(keyPath, 0600); chmodErr != nil {
				return nil, fmt.Errorf("%w: failed to change file mode to 0600: %v", ErrInsecureFilePermissions, chmodErr)
			}
			// Verify chmod succeeded
			if reStat, statErr := os.Stat(keyPath); statErr != nil || reStat.Mode().Perm() != 0600 {
				return nil, ErrInsecureFilePermissions
			}
		}

		keyBytes, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read key file: %w", err)
		}

		// Try loading as encrypted KeyEnvelope JSON
		var env KeyEnvelope
		if jsonErr := json.Unmarshal(keyBytes, &env); jsonErr == nil && env.Algorithm == algorithmName && len(env.Ciphertext) > 0 {
			priv, err := decryptPrivateKey(&env)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt private key: %w", err)
			}
			return priv, nil
		}

		// Try loading as legacy plaintext key bytes
		priv, err := crypto.UnmarshalPrivateKey(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
		}

		// Convert / migrate legacy plaintext key to encrypted format
		if err := savePrivateKeyEncrypted(keyPath, priv); err != nil {
			return nil, fmt.Errorf("failed to migrate key file to encrypted format: %w", err)
		}

		return priv, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to stat key file: %w", err)
	}

	// File does not exist: create directory if needed
	dir := filepath.Dir(keyPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	// Generate a new Ed25519 key
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Encrypt and save key atomically
	if err := savePrivateKeyEncrypted(keyPath, priv); err != nil {
		return nil, fmt.Errorf("failed to save encrypted key: %w", err)
	}

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

// getPassphrase retrieves the passphrase from environment variable CIPHER_IDENTITY_PASSPHRASE or host seed.
func getPassphrase() []byte {
	if passphrase := os.Getenv("CIPHER_IDENTITY_PASSPHRASE"); passphrase != "" {
		return []byte(passphrase)
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return []byte(host)
	}
	return []byte("cipher-default-host-seed")
}

// deriveKey derives a 32-byte AES key using PBKDF2-HMAC-SHA256 with 100,000 iterations.
func deriveKey(salt []byte) []byte {
	passphrase := getPassphrase()
	return pbkdf2.Key(passphrase, salt, pbkdf2Iterations, pbkdf2KeyLen, sha256.New)
}

// encryptPrivateKey marshals and encrypts the private key using AES-256-GCM and returns a KeyEnvelope.
func encryptPrivateKey(priv crypto.PrivKey) (*KeyEnvelope, error) {
	rawBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	salt := make([]byte, saltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	aesKey := deriveKey(salt)
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, rawBytes, nil)

	return &KeyEnvelope{
		Algorithm:  algorithmName,
		Salt:       salt,
		Nonce:      nonce,
		Ciphertext: ciphertext,
	}, nil
}

// decryptPrivateKey decrypts the ciphertext in the KeyEnvelope and unmarshals the libp2p private key.
func decryptPrivateKey(env *KeyEnvelope) (crypto.PrivKey, error) {
	if env.Algorithm != algorithmName {
		return nil, fmt.Errorf("unsupported encryption algorithm: %s", env.Algorithm)
	}

	aesKey := deriveKey(env.Salt)
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	rawBytes, err := gcm.Open(nil, env.Nonce, env.Ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt ciphertext: %w", err)
	}

	priv, err := crypto.UnmarshalPrivateKey(rawBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	return priv, nil
}

// savePrivateKeyEncrypted encrypts a private key and writes it atomically to keyPath with mode 0600.
func savePrivateKeyEncrypted(keyPath string, priv crypto.PrivKey) error {
	env, err := encryptPrivateKey(priv)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal key envelope: %w", err)
	}

	// Write to temporary file first, then atomic rename
	dir := filepath.Dir(keyPath)
	tmpFile, err := os.CreateTemp(dir, "identity-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName) // No-op if rename succeeded
	}()

	if err := os.Chmod(tmpName, 0600); err != nil {
		return fmt.Errorf("failed to set permissions on temp file: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		return fmt.Errorf("failed to write key envelope to temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tmpName, keyPath); err != nil {
		return fmt.Errorf("failed to rename temp file to key file: %w", err)
	}

	// Ensure final file has 0600 permissions
	if err := os.Chmod(keyPath, 0600); err != nil {
		return fmt.Errorf("failed to set 0600 permissions on key file: %w", err)
	}

	return nil
}
