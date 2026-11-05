package engine

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ExportKey writes key material to path with 0600 file permissions and 0700 directory permissions.
func ExportKey(path string, key []byte) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("failed to create directory for key export: %w", err)
		}
		_ = os.Chmod(dir, 0700)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open key output file: %w", err)
	}

	if _, err := f.Write(key); err != nil {
		f.Close()
		return fmt.Errorf("failed to write key to output file: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync key output file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close key output file: %w", err)
	}

	return os.Chmod(path, 0600)
}

// LoadKeyFromFile reads a 32-byte key from binary or hex-encoded key file.
func LoadKeyFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file %s: %w", path, err)
	}

	// If raw binary 32 bytes
	if len(data) == 32 {
		key := make([]byte, 32)
		copy(key, data)
		return key, nil
	}

	// Try parsing hex string (trimmed)
	str := strings.TrimSpace(string(data))
	decoded, err := hex.DecodeString(str)
	if err == nil && len(decoded) == 32 {
		return decoded, nil
	}

	return nil, fmt.Errorf("invalid key file format at %s: must be 32 raw bytes or 64-character hex string", path)
}
