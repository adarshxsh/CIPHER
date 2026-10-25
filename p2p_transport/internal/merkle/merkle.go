package merkle

import (
	"crypto/sha256"

	"cipher/internal/content/core"
)

// ValidateRoot validates a merkle root against a list of chunk IDs
func ValidateRoot(root core.Hash, chunkIDs []core.ChunkID) bool {
	if len(chunkIDs) == 0 {
		return false
	}

	// Milestone 7 uses a simple sha256 hash of concatenated chunk IDs
	// In the future this will be an actual Merkle Tree root.
	var idConcat []byte
	for _, id := range chunkIDs {
		idConcat = append(idConcat, id[:]...)
	}

	hasher := sha256.New()
	hasher.Write(idConcat)
	var calcHash core.Hash
	copy(calcHash[:], hasher.Sum(nil))

	return root == calcHash
}
