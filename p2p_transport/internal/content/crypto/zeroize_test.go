package crypto

import (
	"bytes"
	"testing"
)

func TestZeroize(t *testing.T) {
	key := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0xaa, 0xbb, 0xcc}
	expected := make([]byte, len(key))

	Zeroize(key)

	if !bytes.Equal(key, expected) {
		t.Errorf("expected zeroized slice %v, got %v", expected, key)
	}

	// Test nil and empty slice safety
	Zeroize(nil)
	Zeroize([]byte{})
}
