package keyutil

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"cipher/internal/content/core"
)

// ExportKey writes the given 32-byte key to keyPath with POSIX mode 0600 permissions.
func ExportKey(keyPath string, key []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("invalid key length: expected 32 bytes, got %d", len(key))
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for key file: %w", err)
	}
	f, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create key file: %w", err)
	}
	if _, err := f.Write(key); err != nil {
		f.Close()
		return fmt.Errorf("failed to write key file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close key file: %w", err)
	}
	if err := os.Chmod(keyPath, 0600); err != nil {
		return fmt.Errorf("failed to set 0600 permissions on key file: %w", err)
	}
	return nil
}

// LoadKey loads a 32-byte key from either a file path or an inline 64-character hex string.
func LoadKey(keyInput string) ([]byte, error) {
	keyInput = strings.TrimSpace(keyInput)
	if keyInput == "" {
		return nil, fmt.Errorf("key input is empty")
	}

	// 1. Try reading as a file path if file exists
	if data, err := os.ReadFile(keyInput); err == nil {
		dataStr := strings.TrimSpace(string(data))
		// Check if file contains a 64-char hex string
		if len(dataStr) == 64 {
			if b, err := hex.DecodeString(dataStr); err == nil && len(b) == 32 {
				return b, nil
			}
		}
		// Check if file contains raw 32 bytes
		if len(data) == 32 {
			return data, nil
		}
		// Fallback hex decode
		if b, err := hex.DecodeString(dataStr); err == nil && len(b) == 32 {
			return b, nil
		}
		return nil, fmt.Errorf("key file %s does not contain a valid 32-byte key or 64-character hex string", keyInput)
	}

	// 2. Try parsing as inline hex string
	b, err := hex.DecodeString(keyInput)
	if err != nil || len(b) != 32 {
		return nil, fmt.Errorf("invalid key: must be a path to a key file or a 64-character hex string")
	}
	return b, nil
}

// DefaultKeyPath returns the default path for saving a content decryption key.
func DefaultKeyPath(storePath string, contentID core.ContentID) string {
	return filepath.Join(storePath, fmt.Sprintf("%x.key", contentID))
}
