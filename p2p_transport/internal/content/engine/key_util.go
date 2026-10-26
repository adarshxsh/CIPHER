package engine

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// ExportKey writes raw key bytes to the specified path with 0600 file permissions.
func ExportKey(path string, key []byte) error {
	if path == "" {
		return nil
	}
	return os.WriteFile(path, key, 0600)
}

// FormatKeyFingerprint returns a masked key string (e.g., "a1b2c3d4... [masked]") or "[REDACTED]".
func FormatKeyFingerprint(key []byte) string {
	if len(key) < 4 {
		return "[REDACTED]"
	}
	return fmt.Sprintf("%x... [masked]", key[:4])
}

// ParseKey loads a 32-byte key from either a keyfile path or a hex string.
func ParseKey(keyStr string) ([]byte, error) {
	keyStr = strings.TrimSpace(keyStr)
	if keyStr == "" {
		return nil, fmt.Errorf("empty key string")
	}

	// Try reading as file first
	data, err := os.ReadFile(keyStr)
	if err == nil {
		if len(data) == 32 {
			return data, nil
		}
		fileStr := strings.TrimSpace(string(data))
		decoded, err := hex.DecodeString(fileStr)
		if err == nil && len(decoded) == 32 {
			return decoded, nil
		}
	}

	// Otherwise, decode directly from hex string
	decoded, err := hex.DecodeString(keyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid key hex format or file path: %w", err)
	}
	if len(decoded) != 32 {
		return nil, fmt.Errorf("invalid key length: got %d bytes, expected 32", len(decoded))
	}
	return decoded, nil
}
