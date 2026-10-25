package chunk

import (
	"crypto/sha256"
	"errors"
	"testing"

	"cipher/internal/content/core"
)

func FuzzVerifyIntegrity(f *testing.F) {
	// Add some seeds
	f.Add([]byte{})
	f.Add([]byte("a"))
	f.Add([]byte("test chunk data"))

	f.Fuzz(func(t *testing.T, data []byte) {
		var expectedHash core.Hash

		if len(data) > 0 {
			hasher := sha256.New()
			hasher.Write(data)
			copy(expectedHash[:], hasher.Sum(nil))

			err := VerifyIntegrity(data, expectedHash)
			if err != nil {
				t.Errorf("VerifyIntegrity failed for valid data: %v", err)
			}

			// Test with mismatched hash
			var mismatchedHash core.Hash
			mismatchedHash[0] = expectedHash[0] ^ 0xff
			err = VerifyIntegrity(data, mismatchedHash)
			if !errors.Is(err, ErrIntegrityMismatch) {
				t.Errorf("Expected ErrIntegrityMismatch, got %v", err)
			}
		} else {
			err := VerifyIntegrity(data, expectedHash)
			if !errors.Is(err, ErrEmptyChunkData) {
				t.Errorf("Expected ErrEmptyChunkData, got %v", err)
			}
		}
	})
}
