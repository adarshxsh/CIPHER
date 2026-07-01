package identity

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// LoadOrGenerateKey reads a private key from the given path.
// If the file does not exist, it generates a new Ed25519 key and saves it to the path.
func LoadOrGenerateKey(path string) (crypto.PrivKey, error) {
	// Try to open the file
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// File doesn't exist, generate a new key
			return GenerateAndSaveKey(path)
		}
		return nil, fmt.Errorf("failed to open key file: %w", err)
	}
	defer file.Close()

	// Read the raw bytes
	keyBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	// Unmarshal the key
	privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	return privKey, nil
}

// GenerateAndSaveKey generates a new Ed25519 private key and saves it to the specified path.
func GenerateAndSaveKey(path string) (crypto.PrivKey, error) {
	// Generate an Ed25519 key pair
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ed25519 key: %w", err)
	}

	// Marshal to bytes
	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	// Save to file with restricted permissions (0600)
	if err := os.WriteFile(path, keyBytes, 0600); err != nil {
		return nil, fmt.Errorf("failed to save key to file %s: %w", path, err)
	}

	return privKey, nil
}
