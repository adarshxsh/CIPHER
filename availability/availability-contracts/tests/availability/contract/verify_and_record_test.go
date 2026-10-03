package contract_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	contract "cipher/availability/availability-contracts/contract"
	availabilitytypes "cipher/availability/availability-contracts/types"
	verification "cipher/availability/availability-contracts/verification"
)

type verifyChunkResolver struct{}

func (verifyChunkResolver) ChunkCount(providerID, fileID string) (int, error) {
	return 4, nil
}

func buildTestTree(chunks [][]byte) ([][]byte, []byte, map[int][][]byte) {
	var leaves [][]byte
	for _, c := range chunks {
		h := sha256.Sum256(c)
		leaf := make([]byte, 32)
		copy(leaf, h[:])
		leaves = append(leaves, leaf)
	}

	hNode := func(l, r []byte) []byte {
		hasher := sha256.New()
		hasher.Write(l)
		hasher.Write(r)
		return hasher.Sum(nil)
	}

	p0 := hNode(leaves[0], leaves[1])
	p1 := hNode(leaves[2], leaves[3])
	root := hNode(p0, p1)

	proofs := map[int][][]byte{
		0: {leaves[1], p1},
		1: {leaves[0], p1},
		2: {leaves[3], p0},
		3: {leaves[2], p0},
	}
	return leaves, root, proofs
}

func TestVerifyAndRecordChallengeResponse_PassAndFail(t *testing.T) {
	if err := contract.ConfigureChunkCountResolver(verifyChunkResolver{}); err != nil {
		t.Fatalf("failed to configure chunk resolver: %v", err)
	}

	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	chunks := [][]byte{
		[]byte("block-0"),
		[]byte("block-1"),
		[]byte("block-2"),
		[]byte("block-3"),
	}
	leaves, merkleRoot, proofs := buildTestTree(chunks)

	// 1. Create and fund contract
	contractID, err := contract.CreateAvailabilityContract("publisher-1", "provider-1", "file-1", 100, time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract failed: %v", err)
	}
	if _, err := contract.FundAvailabilityContract(contractID, 100); err != nil {
		t.Fatalf("FundAvailabilityContract failed: %v", err)
	}

	// 2. Initiate Challenge
	challengeID, _, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("InitiateAvailabilityChallenge failed: %v", err)
	}

	// Fetch initiated challenge parameters
	// In the real system, challenge fields are transmitted to the provider
	challenge := availabilitytypes.Challenge{
		ChallengeID: challengeID,
		ContractID:  contractID,
		ProviderID:  "provider-1",
		FileID:      "file-1",
		ChunkID:     0,
		Nonce:       []byte("nonce-round-1"),
		CreatedAt:   time.Now().UTC(),
	}

	// 3. Provider generates valid cryptographic response
	chunkHash := leaves[0]
	digest := verification.ComputeProofDigest(chunkHash, challenge.Nonce, challenge.ChallengeID)
	sig := ed25519.Sign(privKey, digest)

	validResp := verification.ChallengeResponse{
		ChallengeID: challengeID,
		ContractID:  contractID,
		ProviderID:  "provider-1",
		FileID:      "file-1",
		ChunkID:     0,
		ChunkHash:   chunkHash,
		MerkleProof: proofs[0],
		Signature:   sig,
		RespondedAt: time.Now().UTC(),
	}

	// 4. Verify and record the valid response
	state, result, err := contract.VerifyAndRecordChallengeResponse(
		contractID,
		challenge,
		validResp,
		merkleRoot,
		pubKey,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("VerifyAndRecordChallengeResponse returned unexpected error: %v", err)
	}
	if state != availabilitytypes.Active {
		t.Fatalf("expected state Active, got %s", state)
	}
	if !result.Succeeded {
		t.Fatalf("expected result Succeeded=true, got false")
	}

	// 5. Check standardized AvailabilityResult
	latestResult, err := contract.GetLatestAvailabilityResult(contractID)
	if err != nil {
		t.Fatalf("GetLatestAvailabilityResult failed: %v", err)
	}
	if latestResult.Result != availabilitytypes.AvailabilityPass {
		t.Fatalf("expected AvailabilityPass, got %s", latestResult.Result)
	}

	// 6. Test second challenge with corrupted proof (Provider fails challenge)
	challengeID2, _, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("InitiateAvailabilityChallenge 2 failed: %v", err)
	}

	challenge2 := availabilitytypes.Challenge{
		ChallengeID: challengeID2,
		ContractID:  contractID,
		ProviderID:  "provider-1",
		FileID:      "file-1",
		ChunkID:     1,
		Nonce:       []byte("nonce-round-2"),
		CreatedAt:   time.Now().UTC(),
	}

	corruptedResp := verification.ChallengeResponse{
		ChallengeID: challengeID2,
		ContractID:  contractID,
		ProviderID:  "provider-1",
		FileID:      "file-1",
		ChunkID:     1,
		ChunkHash:   []byte("wrong-hash-32-bytes-tampered-1"),
		MerkleProof: proofs[1],
		Signature:   sig,
		RespondedAt: time.Now().UTC(),
	}

	_, result2, err := contract.VerifyAndRecordChallengeResponse(
		contractID,
		challenge2,
		corruptedResp,
		merkleRoot,
		pubKey,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("VerifyAndRecordChallengeResponse returned error: %v", err)
	}
	if result2.Succeeded {
		t.Fatalf("expected result Succeeded=false for corrupted proof, got true")
	}

	latestResult2, err := contract.GetLatestAvailabilityResult(contractID)
	if err != nil {
		t.Fatalf("GetLatestAvailabilityResult failed: %v", err)
	}
	if latestResult2.Result != availabilitytypes.AvailabilityFail {
		t.Fatalf("expected AvailabilityFail, got %s", latestResult2.Result)
	}
}
