package merkle

import (
	"crypto/sha256"
	"testing"

	"cipher/internal/content/core"
)

func FuzzValidateRoot(f *testing.F) {
	f.Add([]byte{}, 0)
	f.Add([]byte("a"), 1)
	f.Add([]byte("abc"), 3)

	f.Fuzz(func(t *testing.T, data []byte, numChunks int) {
		if numChunks < 0 || numChunks > 100 {
			return
		}

		var chunkIDs []core.ChunkID
		var idConcat []byte

		for i := 0; i < numChunks; i++ {
			var id core.ChunkID
			copy(id[:], data)
			chunkIDs = append(chunkIDs, id)
			idConcat = append(idConcat, id[:]...)
		}

		hasher := sha256.New()
		hasher.Write(idConcat)
		var calcHash core.Hash
		copy(calcHash[:], hasher.Sum(nil))

		// Should be true when it's the correct hash and numChunks > 0
		if numChunks > 0 {
			if !ValidateRoot(calcHash, chunkIDs) {
				t.Errorf("ValidateRoot returned false for valid root")
			}
		} else {
			if ValidateRoot(calcHash, chunkIDs) {
				t.Errorf("ValidateRoot returned true for empty chunkIDs")
			}
		}

		// Should be false for an invalid hash
		var invalidHash core.Hash
		invalidHash[0] = calcHash[0] ^ 0xff // flip a bit
		if ValidateRoot(invalidHash, chunkIDs) {
			t.Errorf("ValidateRoot returned true for invalid root")
		}
	})
}
