package identity

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/mattn/go-isatty"
	"golang.org/x/crypto/scrypt"
	"golang.org/x/term"
)

// ScryptParams holds parameters for Scrypt key derivation.
type ScryptParams struct {
	Salt   []byte `json:"salt"`
	N      int    `json:"n"`
	R      int    `json:"r"`
	P      int    `json:"p"`
	KeyLen int    `json:"key_len"`
}

// EncryptedKeyEnvelope defines the encrypted key container format on disk.
type EncryptedKeyEnvelope struct {
	Version      int          `json:"version"`
	KDF          string       `json:"kdf"`
	ScryptParams ScryptParams `json:"scrypt_params"`
	Cipher       string       `json:"cipher"`
	Nonce        []byte       `json:"nonce"`
	Ciphertext   []byte       `json:"ciphertext"`
	AuthTag      []byte       `json:"auth_tag,omitempty"`
}

// VerifyPermissions checks POSIX permission bits on the file and parent directory.
// Rejects files with group/world permissions (mode & 0077 != 0).
func VerifyPermissions(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to stat file %s: %w", path, err)
	}

	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("insecure key file permissions on %s: mode is %04o, must be 0600 or stricter", path, info.Mode().Perm())
	}

	parentDir := filepath.Dir(path)
	dirInfo, err := os.Stat(parentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to stat parent directory %s: %w", parentDir, err)
	}

	if dirInfo.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("insecure parent directory permissions on %s: mode is %04o, must be 0700 or stricter", parentDir, dirInfo.Mode().Perm())
	}

	return nil
}

// ResolvePassphrase resolves the encryption/decryption passphrase.
// Precedence: explicit passphrase -> CIPHER_IDENTITY_PASSPHRASE env -> interactive CLI prompt -> deterministic fallback.
func ResolvePassphrase(explicitPassphrase ...string) (string, error) {
	for _, p := range explicitPassphrase {
		if p != "" {
			return p, nil
		}
	}

	if envPass := os.Getenv("CIPHER_IDENTITY_PASSPHRASE"); envPass != "" {
		return envPass, nil
	}

	if isatty.IsTerminal(os.Stdin.Fd()) || isatty.IsCygwinTerminal(os.Stdin.Fd()) {
		fmt.Fprint(os.Stderr, "Enter identity passphrase: ")
		bytePass, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err == nil && len(bytePass) > 0 {
			return string(bytePass), nil
		}
	}

	return getDeterministicFallbackPassphrase(), nil
}

func getDeterministicFallbackPassphrase() string {
	var sb strings.Builder
	if host, err := os.Hostname(); err == nil {
		sb.WriteString(host)
	}
	if userDir, err := os.UserConfigDir(); err == nil {
		sb.WriteString(userDir)
	}
	if machineID, err := os.ReadFile("/etc/machine-id"); err == nil {
		sb.Write(machineID)
	} else if machineIDWin, err := os.ReadFile("/etc/mid"); err == nil {
		sb.Write(machineIDWin)
	}
	if user := os.Getenv("USER"); user != "" {
		sb.WriteString(user)
	}
	if sb.Len() == 0 {
		sb.WriteString("cipher-default-deterministic-fallback-key")
	}

	hash := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(hash[:])
}

// EncryptKey encrypts an Ed25519 private key into an EncryptedKeyEnvelope using Scrypt KDF and AES-256-GCM.
func EncryptKey(priv crypto.PrivKey, passphrase string) (*EncryptedKeyEnvelope, error) {
	keyBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	salt := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("failed to generate random salt: %w", err)
	}

	params := ScryptParams{
		Salt:   salt,
		N:      32768,
		R:      8,
		P:      1,
		KeyLen: 32,
	}

	derivedKey, err := scrypt.Key([]byte(passphrase), params.Salt, params.N, params.R, params.P, params.KeyLen)
	if err != nil {
		return nil, fmt.Errorf("failed to derive key using scrypt: %w", err)
	}

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM AEAD: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate random nonce: %w", err)
	}

	sealed := gcm.Seal(nil, nonce, keyBytes, nil)
	overhead := gcm.Overhead()
	ciphertext := sealed[:len(sealed)-overhead]
	authTag := sealed[len(sealed)-overhead:]

	envelope := &EncryptedKeyEnvelope{
		Version:      1,
		KDF:          "scrypt",
		ScryptParams: params,
		Cipher:       "aes-256-gcm",
		Nonce:        nonce,
		Ciphertext:   ciphertext,
		AuthTag:      authTag,
	}

	return envelope, nil
}

// DecryptKey decrypts an EncryptedKeyEnvelope using Scrypt KDF and AES-256-GCM.
func DecryptKey(envelope *EncryptedKeyEnvelope, passphrase string) (crypto.PrivKey, error) {
	if envelope.KDF != "scrypt" {
		return nil, fmt.Errorf("unsupported KDF: %s", envelope.KDF)
	}
	if envelope.Cipher != "aes-256-gcm" {
		return nil, fmt.Errorf("unsupported cipher: %s", envelope.Cipher)
	}

	params := envelope.ScryptParams
	derivedKey, err := scrypt.Key([]byte(passphrase), params.Salt, params.N, params.R, params.P, params.KeyLen)
	if err != nil {
		return nil, fmt.Errorf("failed to derive key using scrypt: %w", err)
	}

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM AEAD: %w", err)
	}

	fullCiphertext := append([]byte{}, envelope.Ciphertext...)
	if len(envelope.AuthTag) > 0 {
		fullCiphertext = append(fullCiphertext, envelope.AuthTag...)
	}

	plaintext, err := gcm.Open(nil, envelope.Nonce, fullCiphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt key: invalid passphrase or corrupted key file: %w", err)
	}

	priv, err := crypto.UnmarshalPrivateKey(plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal decrypted private key: %w", err)
	}

	return priv, nil
}

// LoadOrCreate loads an existing key from default user config path or generates a new encrypted container.
func LoadOrCreate(passphrase ...string) (crypto.PrivKey, error) {
	configDir := os.Getenv("CIPHER_CONFIG_DIR")
	if configDir == "" {
		var err error
		configDir, err = os.UserConfigDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get user config dir: %w", err)
		}
	}

	appDir := filepath.Join(configDir, "cipher")
	keyPath := filepath.Join(appDir, "identity.key.enc")
	return LoadOrCreateFromPath(keyPath, passphrase...)
}

// LoadOrCreateWithPassphrase loads or creates a key with explicit passphrase.
func LoadOrCreateWithPassphrase(passphrase string) (crypto.PrivKey, error) {
	return LoadOrCreate(passphrase)
}

// LoadOrCreateFromPathWithPassphrase loads or creates a key from path with explicit passphrase.
func LoadOrCreateFromPathWithPassphrase(keyPath string, passphrase string) (crypto.PrivKey, error) {
	return LoadOrCreateFromPath(keyPath, passphrase)
}

// LoadOrCreateFromPath loads an encrypted Ed25519 key from keyPath, migrates legacy plaintext keys,
// or generates and persists a new encrypted key envelope using strict POSIX permissions (0600 file, 0700 dir).
func LoadOrCreateFromPath(keyPath string, passphrase ...string) (crypto.PrivKey, error) {
	pass, err := ResolvePassphrase(passphrase...)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve passphrase: %w", err)
	}

	var encPath, legacyPath string
	if strings.HasSuffix(keyPath, ".enc") {
		encPath = keyPath
		legacyPath = strings.TrimSuffix(keyPath, ".enc")
	} else if strings.HasSuffix(keyPath, ".key") {
		encPath = keyPath + ".enc"
		legacyPath = keyPath
	} else {
		encPath = keyPath + ".enc"
		legacyPath = keyPath
	}

	// 1. Try loading encrypted key file at encPath
	if _, err := os.Stat(encPath); err == nil {
		if err := VerifyPermissions(encPath); err != nil {
			return nil, err
		}

		keyData, err := os.ReadFile(encPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read encrypted key file: %w", err)
		}

		var envelope EncryptedKeyEnvelope
		if err := json.Unmarshal(keyData, &envelope); err == nil && (envelope.Version > 0 || len(envelope.Ciphertext) > 0) {
			priv, err := DecryptKey(&envelope, pass)
			if err != nil {
				return nil, err
			}
			return priv, nil
		}
	}

	// 2. Try loading legacy key file at legacyPath (or keyPath if custom)
	if _, err := os.Stat(legacyPath); err == nil {
		if err := VerifyPermissions(legacyPath); err != nil {
			return nil, err
		}

		legacyBytes, err := os.ReadFile(legacyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read legacy key file: %w", err)
		}

		// First check if legacyBytes is already an envelope
		var envelope EncryptedKeyEnvelope
		if err := json.Unmarshal(legacyBytes, &envelope); err == nil && (envelope.Version > 0 || len(envelope.Ciphertext) > 0) {
			priv, err := DecryptKey(&envelope, pass)
			if err != nil {
				return nil, err
			}
			return priv, nil
		}

		// Unmarshal legacy plaintext key
		priv, err := crypto.UnmarshalPrivateKey(legacyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal legacy private key: %w", err)
		}

		// Migrate to encrypted envelope format
		newEnvelope, err := EncryptKey(priv, pass)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt key during migration: %w", err)
		}

		encData, err := json.MarshalIndent(newEnvelope, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encrypted key envelope: %w", err)
		}

		dir := filepath.Dir(encPath)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory for encrypted key: %w", err)
		}

		if err := os.WriteFile(encPath, encData, 0600); err != nil {
			return nil, fmt.Errorf("failed to write encrypted key file: %w", err)
		}

		if runtime.GOOS != "windows" {
			_ = os.Chmod(dir, 0700)
			_ = os.Chmod(encPath, 0600)
		}

		// Wipe and remove legacy plaintext key file
		if legacyPath != encPath {
			_ = wipeAndRemoveFile(legacyPath)
		}

		return priv, nil
	}

	// 3. Neither file exists: generate new Ed25519 key pair
	dir := filepath.Dir(encPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	envelope, err := EncryptKey(priv, pass)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt new key: %w", err)
	}

	encData, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new encrypted key: %w", err)
	}

	if err := os.WriteFile(encPath, encData, 0600); err != nil {
		return nil, fmt.Errorf("failed to write encrypted key file: %w", err)
	}

	if runtime.GOOS != "windows" {
		_ = os.Chmod(dir, 0700)
		_ = os.Chmod(encPath, 0600)
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

func wipeAndRemoveFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}
	if info.Size() > 0 {
		zeros := make([]byte, info.Size())
		_ = os.WriteFile(path, zeros, 0600)
	}
	return os.Remove(path)
}
