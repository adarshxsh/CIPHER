package chunk

import (
	"crypto/sha256"
	"errors"

	"cipher/internal/content/core"
)

var (
	ErrIntegrityMismatch = errors.New("chunk integrity mismatch")
	ErrEmptyChunkData    = errors.New("empty chunk data")
)

// VerifyIntegrity hashes the chunk data using SHA-256 and compares it against expectedHash.
func VerifyIntegrity(chunkData []byte, expectedHash core.Hash) error {
	if len(chunkData) == 0 {
		return ErrEmptyChunkData
	}

	hasher := sha256.New()
	hasher.Write(chunkData)

	var actualHash core.Hash
	copy(actualHash[:], hasher.Sum(nil))

	if actualHash != expectedHash {
		return ErrIntegrityMismatch
	}

	return nil
}
