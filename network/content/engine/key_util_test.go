package engine

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestExportKeyAndPermissions(t *testing.T) {
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, "test.key")
	rawKey := []byte("01234567890123456789012345678901") // 32 bytes

	err := ExportKey(keyPath, rawKey)
	if err != nil {
		t.Fatalf("ExportKey failed: %v", err)
	}

	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("Expected permission 0600, got %o", perm)
	}

	readData, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("os.ReadFile failed: %v", err)
	}
	if string(readData) != string(rawKey) {
		t.Errorf("Exported key mismatch")
	}
}

func TestFormatKeyFingerprint(t *testing.T) {
	key := []byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6}
	fmtStr := FormatKeyFingerprint(key)
	expected := "a1b2c3d4... [masked]"
	if fmtStr != expected {
		t.Errorf("Expected %s, got %s", expected, fmtStr)
	}

	shortKey := []byte{0x01, 0x02}
	if FormatKeyFingerprint(shortKey) != "[REDACTED]" {
		t.Errorf("Expected [REDACTED] for short key")
	}
}

func TestParseKey(t *testing.T) {
	rawKey := []byte("12345678901234567890123456789012") // 32 bytes
	hexKey := hex.EncodeToString(rawKey)

	// Test direct hex string
	k1, err := ParseKey(hexKey)
	if err != nil || string(k1) != string(rawKey) {
		t.Fatalf("ParseKey from hex string failed: %v", err)
	}

	// Test raw binary key file
	tempDir := t.TempDir()
	rawFile := filepath.Join(tempDir, "raw.key")
	if err := os.WriteFile(rawFile, rawKey, 0600); err != nil {
		t.Fatalf("Failed to write raw file: %v", err)
	}
	k2, err := ParseKey(rawFile)
	if err != nil || string(k2) != string(rawKey) {
		t.Fatalf("ParseKey from raw file failed: %v", err)
	}

	// Test hex key file
	hexFile := filepath.Join(tempDir, "hex.key")
	if err := os.WriteFile(hexFile, []byte(hexKey+"\n"), 0600); err != nil {
		t.Fatalf("Failed to write hex file: %v", err)
	}
	k3, err := ParseKey(hexFile)
	if err != nil || string(k3) != string(rawKey) {
		t.Fatalf("ParseKey from hex file failed: %v", err)
	}
}
