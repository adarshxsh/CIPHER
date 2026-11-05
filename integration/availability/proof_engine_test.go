package availability

import (
	"crypto/ed25519"
	"testing"
	"time"

	contract "cipher/availability/availability-contracts/contract"
	availabilitytypes "cipher/availability/availability-contracts/types"
)

type dummyResolver struct{ count int }

func (d dummyResolver) ChunkCount(string, string) (int, error) {
	return d.count, nil
}

func TestProviderProofEngine_HappyPathAndAdversarial(t *testing.T) {
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	providerID := "provider-peer-123"
	engine, err := NewProviderProofEngine(providerID, privKey, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	chunks := [][]byte{
		[]byte("cipher-data-chunk-0"),
		[]byte("cipher-data-chunk-1"),
		[]byte("cipher-data-chunk-2"),
		[]byte("cipher-data-chunk-3"),
	}

	fileID := "file-xyz-789"
	tree, err := engine.RegisterFileChunks(fileID, chunks)
	if err != nil {
		t.Fatalf("failed to register file chunks: %v", err)
	}

	root := tree.Root()

	// Configure resolver for contract initiation
	if err := contract.ConfigureChunkCountResolver(dummyResolver{count: len(chunks)}); err != nil {
		t.Fatalf("failed to configure resolver: %v", err)
	}

	// 1. Create and fund contract
	contractID, err := contract.CreateAvailabilityContract("pub-1", providerID, fileID, 1000, 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract failed: %v", err)
	}
	if _, err := contract.FundAvailabilityContract(contractID, 1000); err != nil {
		t.Fatalf("FundAvailabilityContract failed: %v", err)
	}

	// 2. Initiate Challenge
	challengeID, _, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("InitiateAvailabilityChallenge failed: %v", err)
	}

	issuedChallenge := availabilitytypes.Challenge{
		ChallengeID: challengeID,
		ContractID:  contractID,
		ProviderID:  providerID,
		FileID:      fileID,
		ChunkID:     2, // Provable chunk
		Nonce:       []byte("random-entropy-nonce-12345"),
		CreatedAt:   time.Now().UTC(),
	}

	// 3. Provider generates valid proof response
	resp, err := engine.HandleChallenge(issuedChallenge)
	if err != nil {
		t.Fatalf("HandleChallenge failed: %v", err)
	}

	// 4. Publisher verifies and records
	nextState, res, err := contract.VerifyAndRecordChallengeResponse(
		contractID,
		issuedChallenge,
		resp,
		root,
		pubKey,
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("VerifyAndRecordChallengeResponse returned error: %v", err)
	}
	if !res.Succeeded {
		t.Fatalf("expected challenge result to SUCCEED, got FAIL")
	}
	if nextState != availabilitytypes.Active {
		t.Fatalf("expected contract state to be Active, got %s", nextState)
	}

	// 5. Test Adversarial Scenario: Provider sends corrupt chunk hash
	challengeID2, _, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("InitiateAvailabilityChallenge 2 failed: %v", err)
	}

	adversarialChallenge := availabilitytypes.Challenge{
		ChallengeID: challengeID2,
		ContractID:  contractID,
		ProviderID:  providerID,
		FileID:      fileID,
		ChunkID:     1,
		Nonce:       []byte("random-entropy-nonce-67890"),
		CreatedAt:   time.Now().UTC(),
	}

	corruptResp, err := engine.HandleChallengeAdversarial(adversarialChallenge, true, false)
	if err != nil {
		t.Fatalf("HandleChallengeAdversarial failed: %v", err)
	}

	_, failRes, err := contract.VerifyAndRecordChallengeResponse(
		contractID,
		adversarialChallenge,
		corruptResp,
		root,
		pubKey,
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("VerifyAndRecordChallengeResponse error: %v", err)
	}
	if failRes.Succeeded {
		t.Fatalf("expected corrupted response to FAIL, but it SUCCEEDED")
	}
}
