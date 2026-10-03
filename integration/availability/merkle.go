package availability

import (
	"crypto/sha256"
	"errors"
	"fmt"
)

// MerkleTree implements a standard binary SHA-256 Merkle tree designed to generate
// proofs that verify against cipher/availability/availability-contracts/verification.VerifyMerkleProof.
type MerkleTree struct {
	leaves [][]byte
	levels [][][]byte
	root   []byte
}

// NewMerkleTree builds a binary Merkle tree from an ordered slice of 32-byte leaf hashes.
// If an odd number of nodes exists at any level, the last node is duplicated to complete the pair.
func NewMerkleTree(leafHashes [][]byte) (*MerkleTree, error) {
	if len(leafHashes) == 0 {
		return nil, errors.New("cannot build Merkle tree with 0 leaves")
	}

	leaves := make([][]byte, len(leafHashes))
	for i, h := range leafHashes {
		if len(h) != 32 {
			return nil, fmt.Errorf("leaf hash %d has invalid length %d (expected 32)", i, len(h))
		}
		leaves[i] = make([]byte, 32)
		copy(leaves[i], h)
	}

	// Single leaf edge case: root is leaf itself
	if len(leaves) == 1 {
		return &MerkleTree{
			leaves: leaves,
			levels: [][][]byte{leaves},
			root:   leaves[0],
		}, nil
	}

	var levels [][][]byte
	currentLevel := leaves
	levels = append(levels, currentLevel)

	for len(currentLevel) > 1 {
		// If odd length, duplicate last node
		padded := currentLevel
		if len(padded)%2 != 0 {
			padded = append(padded, padded[len(padded)-1])
		}

		var nextLevel [][]byte
		for i := 0; i < len(padded); i += 2 {
			parent := HashPair(padded[i], padded[i+1])
			nextLevel = append(nextLevel, parent)
		}

		levels = append(levels, nextLevel)
		currentLevel = nextLevel
	}

	return &MerkleTree{
		leaves: leaves,
		levels: levels,
		root:   currentLevel[0],
	}, nil
}

// BuildMerkleTreeFromChunks computes SHA-256 hashes for each raw chunk and builds the tree.
func BuildMerkleTreeFromChunks(chunks [][]byte) (*MerkleTree, error) {
	if len(chunks) == 0 {
		return nil, errors.New("no chunks provided")
	}
	leaves := make([][]byte, len(chunks))
	for i, c := range chunks {
		h := sha256.Sum256(c)
		leaves[i] = h[:]
	}
	return NewMerkleTree(leaves)
}

// Root returns the 32-byte Merkle root.
func (m *MerkleTree) Root() []byte {
	rootCopy := make([]byte, 32)
	copy(rootCopy, m.root)
	return rootCopy
}

// LeafCount returns the number of original leaves in the tree.
func (m *MerkleTree) LeafCount() int {
	return len(m.leaves)
}

// GenerateProof produces the sibling audit path hashes for leafIndex.
// The resulting slice can be directly validated with verification.VerifyMerkleProof.
func (m *MerkleTree) GenerateProof(leafIndex int) ([][]byte, error) {
	if leafIndex < 0 || leafIndex >= len(m.leaves) {
		return nil, fmt.Errorf("leafIndex %d out of bounds [0, %d)", leafIndex, len(m.leaves))
	}

	if len(m.leaves) == 1 {
		return [][]byte{}, nil
	}

	var proof [][]byte
	idx := leafIndex

	// Traverse from level 0 up to (total levels - 1)
	for level := 0; level < len(m.levels)-1; level++ {
		currentNodes := m.levels[level]
		if len(currentNodes)%2 != 0 {
			currentNodes = append(currentNodes, currentNodes[len(currentNodes)-1])
		}

		var sibling []byte
		if idx%2 == 0 {
			// idx is even -> sibling is idx + 1
			sibling = currentNodes[idx+1]
		} else {
			// idx is odd -> sibling is idx - 1
			sibling = currentNodes[idx-1]
		}

		proofSibling := make([]byte, 32)
		copy(proofSibling, sibling)
		proof = append(proof, proofSibling)

		idx /= 2
	}

	return proof, nil
}

// HashPair computes SHA256(left || right).
func HashPair(left, right []byte) []byte {
	hasher := sha256.New()
	hasher.Write(left)
	hasher.Write(right)
	return hasher.Sum(nil)
}
