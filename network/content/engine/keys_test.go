package engine

import (
	"encoding/hex"
	"testing"
)

func TestFormatMaskedKey(t *testing.T) {
	// 32-byte secret key (64 hex characters)
	key32 := []byte{
		0xa1, 0xb2, 0xc3, 0xd4, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x3f, 0x4d,
	}

	fullHex := hex.EncodeToString(key32)
	if len(fullHex) != 64 {
		t.Fatalf("expected 64 hex characters, got %d", len(fullHex))
	}

	t.Run("showKey is false", func(t *testing.T) {
		got := FormatMaskedKey(key32, false)
		expected := "a1b2...3f4d"
		if got != expected {
			t.Errorf("FormatMaskedKey(key, false) = %q; want %q", got, expected)
		}
	})

	t.Run("showKey is true", func(t *testing.T) {
		got := FormatMaskedKey(key32, true)
		if got != fullHex {
			t.Errorf("FormatMaskedKey(key, true) = %q; want %q", got, fullHex)
		}
	})

	t.Run("short key showKey false", func(t *testing.T) {
		shortKey := []byte{0x01, 0x02}
		got := FormatMaskedKey(shortKey, false)
		expected := "0102"
		if got != expected {
			t.Errorf("FormatMaskedKey(shortKey, false) = %q; want %q", got, expected)
		}
	})

	t.Run("empty key", func(t *testing.T) {
		var emptyKey []byte
		gotFalse := FormatMaskedKey(emptyKey, false)
		gotTrue := FormatMaskedKey(emptyKey, true)
		if gotFalse != "" || gotTrue != "" {
			t.Errorf("FormatMaskedKey(empty, false)=%q, (empty, true)=%q; want empty string", gotFalse, gotTrue)
		}
	})
}
