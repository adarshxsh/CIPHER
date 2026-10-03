package verification_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	verification "cipher/availability/availability-contracts/verification"
)

// helper to build a 4-leaf Merkle tree
type testMerkleTree struct {
	leaves [][]byte
	root   []byte
	proofs map[int][][]byte
}

func buildTestMerkleTree(chunkContents [][]byte) *testMerkleTree {
	var leaves [][]byte
	for _, chunk := range chunkContents {
		h := sha256.Sum256(chunk)
		leaf := make([]byte, 32)
		copy(leaf, h[:])
		leaves = append(leaves, leaf)
	}

	// Level 1
	p0 := sha256Node(leaves[0], leaves[1])
	p1 := sha256Node(leaves[2], leaves[3])

	// Root
	root := sha256Node(p0, p1)

	proofs := map[int][][]byte{
		0: {leaves[1], p1},
		1: {leaves[0], p1},
		2: {leaves[3], p0},
		3: {leaves[2], p0},
	}

	return &testMerkleTree{
		leaves: leaves,
		root:   root,
		proofs: proofs,
	}
}

func sha256Node(left, right []byte) []byte {
	hasher := sha256.New()
	hasher.Write(left)
	hasher.Write(right)
	return hasher.Sum(nil)
}

func TestVerifyChallengeResponse_Success(t *testing.T) {
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	chunks := [][]byte{
		[]byte("chunk data 0"),
		[]byte("chunk data 1"),
		[]byte("chunk data 2"),
		[]byte("chunk data 3"),
	}
	tree := buildTestMerkleTree(chunks)

	chunkID := 1
	now := time.Now().UTC()
	challenge := availabilitytypes.Challenge{
		ChallengeID: availabilitytypes.ChallengeID("challenge-test-1"),
		ContractID:  availabilitytypes.ContractID("contract-test-1"),
		ProviderID:  "provider-alpha",
		FileID:      "file-alpha",
		ChunkID:     chunkID,
		Nonce:       []byte("random-32-byte-nonce-value-12345"),
		CreatedAt:   now,
	}

	chunkHash := tree.leaves[chunkID]
	digest := verification.ComputeProofDigest(chunkHash, challenge.Nonce, challenge.ChallengeID)
	signature := ed25519.Sign(privKey, digest)

	response := verification.ChallengeResponse{
		ChallengeID: challenge.ChallengeID,
		ContractID:  challenge.ContractID,
		ProviderID:  challenge.ProviderID,
		FileID:      challenge.FileID,
		ChunkID:     chunkID,
		ChunkHash:   chunkHash,
		MerkleProof: tree.proofs[chunkID],
		Signature:   signature,
		RespondedAt: now.Add(time.Second * 2),
	}

	valid, err := verification.VerifyChallengeResponse(challenge, response, tree.root, pubKey, time.Minute)
	if err != nil || !valid {
		t.Fatalf("expected valid challenge response, got valid=%v, err=%v", valid, err)
	}

	result := verification.BuildChallengeResult(challenge, response, tree.root, pubKey, time.Minute)
	if !result.Succeeded || result.ChallengeID != challenge.ChallengeID {
		t.Fatalf("expected succeeded ChallengeResult, got %+v", result)
	}
}

func TestVerifyChallengeResponse_RejectsInvalidMerkleProof(t *testing.T) {
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	chunks := [][]byte{
		[]byte("chunk 0"),
		[]byte("chunk 1"),
		[]byte("chunk 2"),
		[]byte("chunk 3"),
	}
	tree := buildTestMerkleTree(chunks)

	chunkID := 2
	now := time.Now().UTC()
	challenge := availabilitytypes.Challenge{
		ChallengeID: "challenge-test-2",
		ContractID:  "contract-test-2",
		ProviderID:  "provider-beta",
		FileID:      "file-beta",
		ChunkID:     chunkID,
		Nonce:       []byte("random-nonce-xyz"),
		CreatedAt:   now,
	}

	// Corrupted chunk hash (lying about chunk data)
	fakeHash := sha256.Sum256([]byte("fake data"))
	digest := verification.ComputeProofDigest(fakeHash[:], challenge.Nonce, challenge.ChallengeID)
	signature := ed25519.Sign(privKey, digest)

	response := verification.ChallengeResponse{
		ChallengeID: challenge.ChallengeID,
		ContractID:  challenge.ContractID,
		ProviderID:  challenge.ProviderID,
		FileID:      challenge.FileID,
		ChunkID:     chunkID,
		ChunkHash:   fakeHash[:],
		MerkleProof: tree.proofs[chunkID],
		Signature:   signature,
		RespondedAt: now.Add(time.Second),
	}

	valid, err := verification.VerifyChallengeResponse(challenge, response, tree.root, pubKey, time.Minute)
	if valid || err == nil {
		t.Fatalf("expected failure for corrupted chunk hash, got valid=%v, err=%v", valid, err)
	}
}

func TestVerifyChallengeResponse_RejectsReplayAttack(t *testing.T) {
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	chunks := [][]byte{
		[]byte("chunk 0"),
		[]byte("chunk 1"),
		[]byte("chunk 2"),
		[]byte("chunk 3"),
	}
	tree := buildTestMerkleTree(chunks)

	now := time.Now().UTC()
	challenge := availabilitytypes.Challenge{
		ChallengeID: "challenge-fresh",
		ContractID:  "contract-test-3",
		ProviderID:  "provider-gamma",
		FileID:      "file-gamma",
		ChunkID:     0,
		Nonce:       []byte("fresh-challenge-nonce-9999"),
		CreatedAt:   now,
	}

	// Signature signed with an OLD/DIFFERENT nonce (replayed response)
	oldNonce := []byte("old-stale-nonce-1111")
	oldDigest := verification.ComputeProofDigest(tree.leaves[0], oldNonce, challenge.ChallengeID)
	staleSignature := ed25519.Sign(privKey, oldDigest)

	response := verification.ChallengeResponse{
		ChallengeID: challenge.ChallengeID,
		ContractID:  challenge.ContractID,
		ProviderID:  challenge.ProviderID,
		FileID:      challenge.FileID,
		ChunkID:     0,
		ChunkHash:   tree.leaves[0],
		MerkleProof: tree.proofs[0],
		Signature:   staleSignature,
		RespondedAt: now.Add(time.Second),
	}

	valid, err := verification.VerifyChallengeResponse(challenge, response, tree.root, pubKey, time.Minute)
	if valid || err == nil {
		t.Fatalf("expected replay rejection due to mismatched nonce digest, got valid=%v, err=%v", valid, err)
	}
}

func TestVerifyChallengeResponse_RejectsDeadlineExpired(t *testing.T) {
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	chunks := [][]byte{
		[]byte("chunk 0"),
		[]byte("chunk 1"),
		[]byte("chunk 2"),
		[]byte("chunk 3"),
	}
	tree := buildTestMerkleTree(chunks)

	now := time.Now().UTC()
	challenge := availabilitytypes.Challenge{
		ChallengeID: "challenge-test-timeout",
		ContractID:  "contract-test-timeout",
		ProviderID:  "provider-delta",
		FileID:      "file-delta",
		ChunkID:     0,
		Nonce:       []byte("timeout-nonce"),
		CreatedAt:   now,
	}

	digest := verification.ComputeProofDigest(tree.leaves[0], challenge.Nonce, challenge.ChallengeID)
	signature := ed25519.Sign(privKey, digest)

	// Responded after 10 seconds, but max timeout is 5 seconds
	response := verification.ChallengeResponse{
		ChallengeID: challenge.ChallengeID,
		ContractID:  challenge.ContractID,
		ProviderID:  challenge.ProviderID,
		FileID:      challenge.FileID,
		ChunkID:     0,
		ChunkHash:   tree.leaves[0],
		MerkleProof: tree.proofs[0],
		Signature:   signature,
		RespondedAt: now.Add(10 * time.Second),
	}

	valid, err := verification.VerifyChallengeResponse(challenge, response, tree.root, pubKey, 5*time.Second)
	if valid || err == nil {
		t.Fatalf("expected deadline expiration failure, got valid=%v, err=%v", valid, err)
	}
}
