package availability

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	verification "cipher/availability/availability-contracts/verification"
)

func TestMerkleTree_VerifyProofs(t *testing.T) {
	// Test various leaf counts including power of 2, odd, and edge cases
	leafCounts := []int{1, 2, 3, 4, 5, 7, 8, 16, 17, 32}

	for _, count := range leafCounts {
		t.Run("LeafCount", func(t *testing.T) {
			leaves := make([][]byte, count)
			for i := 0; i < count; i++ {
				data := make([]byte, 64)
				rand.Read(data)
				h := sha256.Sum256(data)
				leaves[i] = h[:]
			}

			tree, err := NewMerkleTree(leaves)
			if err != nil {
				t.Fatalf("failed to build tree with %d leaves: %v", count, err)
			}

			root := tree.Root()
			if len(root) != 32 {
				t.Fatalf("invalid root size: %d", len(root))
			}

			for i := 0; i < count; i++ {
				proof, err := tree.GenerateProof(i)
				if err != nil {
					t.Fatalf("failed to generate proof for leaf %d: %v", i, err)
				}

				// Verify using the availability subsystem's verification.VerifyMerkleProof
				ok := verification.VerifyMerkleProof(leaves[i], i, proof, root)
				if !ok {
					t.Fatalf("verification.VerifyMerkleProof returned false for leaf %d/%d", i, count)
				}

				// Adversarial check: tampered leaf must fail
				tamperedLeaf := make([]byte, 32)
				copy(tamperedLeaf, leaves[i])
				tamperedLeaf[0] ^= 0xFF
				if verification.VerifyMerkleProof(tamperedLeaf, i, proof, root) {
					t.Fatalf("tampered leaf unexpectedly passed for leaf %d/%d", i, count)
				}
			}
		})
	}
}
