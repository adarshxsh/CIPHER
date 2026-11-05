// Package verification implements cryptographic proof verification for Availability challenges.
//
// In the CIPHER CDN architecture:
// 1. A file is divided into chunks committed to a publisher-signed Merkle root.
// 2. An availability challenge issues a specific ChunkID and a one-time cryptographic Nonce.
// 3. The provider must prove:
//   - Possession of the chunk data by providing its hash and a valid Merkle audit path to the root.
//   - Liveness/Freshness by signing or binding the chunk hash to the challenge's one-time Nonce.
//   - Timeliness by responding before the challenge deadline.
package verification

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
)

// ChallengeResponse represents the cryptographic response submitted by a provider
// to prove it still stores and can serve the requested chunk.
type ChallengeResponse struct {
	ChallengeID availabilitytypes.ChallengeID // Must match the issued challenge
	ContractID  availabilitytypes.ContractID  // Must match the active availability contract
	ProviderID  string                        // Provider peer identity answering the challenge
	FileID      string                        // Target file identity
	ChunkID     int                           // 0-indexed chunk identifier being proven
	ChunkHash   []byte                        // SHA-256 hash of the local chunk content
	MerkleProof [][]byte                      // Sibling audit path hashes from chunk leaf up to Merkle root
	Signature   []byte                        // Ed25519 signature of the provider over the proof-of-possession digest
	RespondedAt time.Time                     // Timestamp when provider signed/issued the response
}

// ComputeProofDigest derives the deterministic binding digest that ties the chunk hash
// to the challenge's one-time nonce and identifier:
//
//	Digest = SHA256(ChunkHash || Nonce || ChallengeID)
//
// This guarantees replay protection: a provider cannot reuse a past chunk proof for
// a new challenge because the challenger supplies fresh entropy in Nonce.
func ComputeProofDigest(chunkHash []byte, nonce []byte, challengeID availabilitytypes.ChallengeID) []byte {
	hasher := sha256.New()
	hasher.Write(chunkHash)
	hasher.Write(nonce)
	hasher.Write([]byte(challengeID))
	return hasher.Sum(nil)
}

// VerifyMerkleProof validates that leafHash belongs to expectedRoot at leafIndex.
//
// Standard binary Merkle path verification:
// At each tree level, if current index is even, current node is the left child:
//
//	parent = SHA256(current || sibling)
//
// If current index is odd, current node is the right child:
//
//	parent = SHA256(sibling || current)
//
// The index is halved for the next level until the root is reached.
func VerifyMerkleProof(leafHash []byte, leafIndex int, proof [][]byte, expectedRoot []byte) bool {
	if len(leafHash) != 32 || len(expectedRoot) != 32 || leafIndex < 0 {
		return false
	}

	current := make([]byte, 32)
	copy(current, leafHash)
	idx := leafIndex

	for _, sibling := range proof {
		if len(sibling) != 32 {
			return false
		}

		hasher := sha256.New()
		if idx%2 == 0 {
			// Current is left child, sibling is right child
			hasher.Write(current)
			hasher.Write(sibling)
		} else {
			// Current is right child, sibling is left child
			hasher.Write(sibling)
			hasher.Write(current)
		}
		current = hasher.Sum(nil)
		idx /= 2
	}

	return bytes.Equal(current, expectedRoot)
}

// VerifyChallengeResponse performs full end-to-end cryptographic and liveness validation
// on a provider's challenge response.
//
// Verification steps:
// 1. Structural validation (matching ChallengeID, ChunkID, ProviderID, FileID).
// 2. Freshness check (response submitted within maxTimeout).
// 3. Content proof check (valid Merkle path from ChunkHash to file Merkle root).
// 4. Identity & Liveness check (valid Ed25519 signature over ComputeProofDigest).
func VerifyChallengeResponse(
	challenge availabilitytypes.Challenge,
	response ChallengeResponse,
	expectedMerkleRoot []byte,
	providerPubKey ed25519.PublicKey,
	maxTimeout time.Duration,
) (bool, error) {
	// 1. Identifier consistency checks
	if challenge.ChallengeID == "" {
		return false, errors.New("challenge ID cannot be empty")
	}
	if response.ChallengeID != challenge.ChallengeID {
		return false, fmt.Errorf("response challenge ID %s does not match expected %s", response.ChallengeID, challenge.ChallengeID)
	}
	if response.ContractID != challenge.ContractID {
		return false, fmt.Errorf("response contract ID %s does not match expected %s", response.ContractID, challenge.ContractID)
	}
	if response.ProviderID != challenge.ProviderID {
		return false, fmt.Errorf("response provider ID %s does not match expected %s", response.ProviderID, challenge.ProviderID)
	}
	if response.FileID != challenge.FileID {
		return false, fmt.Errorf("response file ID %s does not match expected %s", response.FileID, challenge.FileID)
	}
	if response.ChunkID != challenge.ChunkID {
		return false, fmt.Errorf("response chunk ID %d does not match expected %d", response.ChunkID, challenge.ChunkID)
	}

	// 2. Freshness and deadline validation
	if maxTimeout > 0 {
		deadline := challenge.CreatedAt.Add(maxTimeout)
		if response.RespondedAt.After(deadline) {
			return false, fmt.Errorf("response received after deadline (elapsed %v > max %v)", response.RespondedAt.Sub(challenge.CreatedAt), maxTimeout)
		}
	}

	// 3. Merkle audit path proof validation
	if len(expectedMerkleRoot) != 32 {
		return false, errors.New("expected Merkle root must be a 32-byte SHA-256 hash")
	}
	if !VerifyMerkleProof(response.ChunkHash, response.ChunkID, response.MerkleProof, expectedMerkleRoot) {
		return false, errors.New("Merkle proof verification failed: chunk hash does not belong to file Merkle root")
	}

	// 4. Provider signature & replay protection validation
	if len(providerPubKey) > 0 {
		if len(providerPubKey) != ed25519.PublicKeySize {
			return false, errors.New("invalid provider Ed25519 public key size")
		}
		if len(response.Signature) != ed25519.SignatureSize {
			return false, errors.New("invalid provider signature size")
		}

		digest := ComputeProofDigest(response.ChunkHash, challenge.Nonce, challenge.ChallengeID)
		if !ed25519.Verify(providerPubKey, digest, response.Signature) {
			return false, errors.New("provider signature verification failed: signature does not match proof digest")
		}
	}

	return true, nil
}

// BuildChallengeResult evaluates the response and returns a standardized ChallengeResult
// ready to be recorded in the contract store.
func BuildChallengeResult(
	challenge availabilitytypes.Challenge,
	response ChallengeResponse,
	expectedMerkleRoot []byte,
	providerPubKey ed25519.PublicKey,
	maxTimeout time.Duration,
) availabilitytypes.ChallengeResult {
	valid, _ := VerifyChallengeResponse(challenge, response, expectedMerkleRoot, providerPubKey, maxTimeout)
	return availabilitytypes.ChallengeResult{
		ChallengeID: challenge.ChallengeID,
		Succeeded:   valid,
		RecordedAt:  time.Now().UTC(),
	}
}
