package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cipher/internal/content/core"
)

// FSKeyProvider implements core.KeyProvider storing 32-byte decryption keys
// in individual files with 0600 permissions under <baseDir>/keys/.
type FSKeyProvider struct {
	baseDir string
	mu      sync.RWMutex
}

func NewFSKeyProvider(baseDir string) *FSKeyProvider {
	return &FSKeyProvider{
		baseDir: baseDir,
	}
}

func (p *FSKeyProvider) keyDir() string {
	return filepath.Join(p.baseDir, "keys")
}

func (p *FSKeyProvider) keyPath(id core.ContentID) string {
	encoded := hex.EncodeToString(id[:])
	return filepath.Join(p.keyDir(), encoded+".key")
}

func (p *FSKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	path := p.keyPath(id)
	key, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("key not found")
		}
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	if len(key) != 32 {
		return nil, fmt.Errorf("invalid key size in file %s: expected 32 bytes, got %d", path, len(key))
	}

	keyCopy := make([]byte, 32)
	copy(keyCopy, key)
	return keyCopy, nil
}

func (p *FSKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(key) != 32 {
		return fmt.Errorf("invalid key size: expected 32 bytes, got %d", len(key))
	}

	dir := p.keyDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create key dir: %w", err)
	}

	finalPath := p.keyPath(id)

	// Atomic write: write to temp file first, set permissions 0600, sync, close, rename
	tmpFile, err := os.CreateTemp(dir, ".key-tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temp key file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		os.Remove(tmpName)
	}()

	if err := tmpFile.Chmod(0600); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to set key file permissions: %w", err)
	}

	if _, err := tmpFile.Write(key); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write key data: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to sync key file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp key file: %w", err)
	}

	if err := os.Rename(tmpName, finalPath); err != nil {
		return fmt.Errorf("failed to rename temp key file: %w", err)
	}

	return nil
}

func (p *FSKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	path := p.keyPath(id)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Secure wipe: overwrite file content with zeroes before deletion
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err == nil {
		zeroes := make([]byte, info.Size())
		_, _ = f.Write(zeroes)
		_ = f.Sync()
		f.Close()
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete key file: %w", err)
	}

	return nil
}

// KeyFingerprint returns a masked, truncated SHA-256 fingerprint preview of the key.
func KeyFingerprint(key []byte) string {
	fp := sha256.Sum256(key)
	return hex.EncodeToString(fp[:6])
}

// LoadKeyFromFile reads a decryption key from a file path or stdin ("-").
// Supports both 32-byte raw binary keys and 64-character hex strings.
func LoadKeyFromFile(path string) ([]byte, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read key file %s: %w", path, err)
	}
	return ParseKeyBytes(data)
}

// ParseKeyBytes parses a 32-byte raw binary or 64-hex char key.
func ParseKeyBytes(data []byte) ([]byte, error) {
	if len(data) == 32 {
		keyCopy := make([]byte, 32)
		copy(keyCopy, data)
		return keyCopy, nil
	}
	if (len(data) == 33 && (data[32] == '\n' || data[32] == '\r')) ||
		(len(data) == 34 && data[32] == '\r' && data[33] == '\n') {
		keyCopy := make([]byte, 32)
		copy(keyCopy, data[:32])
		return keyCopy, nil
	}
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) == 64 {
		key, err := hex.DecodeString(trimmed)
		if err == nil && len(key) == 32 {
			return key, nil
		}
	}
	key, err := hex.DecodeString(trimmed)
	if err == nil && len(key) == 32 {
		return key, nil
	}
	return nil, fmt.Errorf("invalid key data: expected 32 raw bytes or 64 hex characters, got %d bytes", len(data))
}

// ExportKeyToFile writes key bytes to a file with 0600 POSIX permissions.
func ExportKeyToFile(path string, key []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("invalid key size for export: expected 32 bytes, got %d", len(key))
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory for key export: %w", err)
		}
	}
	if err := os.WriteFile(path, key, 0600); err != nil {
		return fmt.Errorf("failed to export key to file: %w", err)
	}
	return nil
}
