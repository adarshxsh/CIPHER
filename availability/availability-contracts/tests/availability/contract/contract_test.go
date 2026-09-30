package contract_test

import (
	"errors"
	"testing"
	"time"

	challenge "cipher/availability/availability-contracts/challenge"
	contract "cipher/availability/availability-contracts/contract"
	availabilitytypes "cipher/availability/availability-contracts/types"
)

type fixedChunkCountResolver struct{ count int }

func (r fixedChunkCountResolver) ChunkCount(providerID, fileID string) (int, error) {
	if providerID == "" || fileID == "" {
		return 0, errors.New("provider ID and file ID are required")
	}
	return r.count, nil
}

func TestCreateAndFundAvailabilityContract(t *testing.T) {
	if err := contract.ConfigureChunkCountResolver(fixedChunkCountResolver{count: 3}); err != nil {
		t.Fatalf("ConfigureChunkCountResolver returned error: %v", err)
	}
	contractID, err := contract.CreateAvailabilityContract("publisher-contract", "provider-contract", "file-contract", 100, time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract returned error: %v", err)
	}
	if contractID == "" {
		t.Fatal("CreateAvailabilityContract returned an empty ID")
	}
	state, err := contract.FundAvailabilityContract(contractID, 100)
	if err != nil {
		t.Fatalf("FundAvailabilityContract returned error: %v", err)
	}
	if state != availabilitytypes.Agreed {
		t.Fatalf("funding state = %s, want %s", state, availabilitytypes.Agreed)
	}
	if _, err := contract.FundAvailabilityContract(contractID, 100); err == nil {
		t.Fatal("duplicate funding succeeded")
	}
}

func TestContractRejectsIncorrectFundingAndRepeatedTermination(t *testing.T) {
	if err := contract.ConfigureChunkCountResolver(fixedChunkCountResolver{count: 2}); err != nil {
		t.Fatalf("ConfigureChunkCountResolver returned error: %v", err)
	}
	contractID, err := contract.CreateAvailabilityContract("publisher-terminate", "provider-terminate", "file-terminate", 100, time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract returned error: %v", err)
	}
	if _, err := contract.FundAvailabilityContract(contractID, 99); err == nil {
		t.Fatal("FundAvailabilityContract accepted an incorrect amount")
	}
	state, err := contract.TerminateAvailabilityContract(contractID)
	if err != nil || state != availabilitytypes.Terminated {
		t.Fatalf("TerminateAvailabilityContract = %s, %v; want TERMINATED, nil", state, err)
	}
	if _, err := contract.TerminateAvailabilityContract(contractID); err == nil {
		t.Fatal("repeated termination succeeded")
	}
}

func TestContractRejectsInvalidOperations(t *testing.T) {
	if err := contract.ConfigureChunkCountResolver(nil); err == nil {
		t.Fatal("ConfigureChunkCountResolver accepted nil")
	}
	if _, err := contract.CreateAvailabilityContract("", "provider", "file", 1, time.Hour); err == nil {
		t.Fatal("CreateAvailabilityContract accepted an empty publisher ID")
	}
	if _, err := contract.CreateAvailabilityContract("publisher", "provider", "file", 0, time.Hour); err == nil {
		t.Fatal("CreateAvailabilityContract accepted a zero payment")
	}
	if _, err := contract.FundAvailabilityContract("unknown-contract", 1); err == nil {
		t.Fatal("FundAvailabilityContract accepted an unknown contract")
	}
}

func TestInitiateChallengeRejectsExpiredContract(t *testing.T) {
	if err := contract.ConfigureChunkCountResolver(fixedChunkCountResolver{count: 2}); err != nil {
		t.Fatalf("ConfigureChunkCountResolver returned error: %v", err)
	}
	contractID, err := contract.CreateAvailabilityContract("publisher-expired", "provider-expired", "file-expired", 10, time.Nanosecond)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract returned error: %v", err)
	}
	if _, err := contract.FundAvailabilityContract(contractID, 10); err != nil {
		t.Fatalf("FundAvailabilityContract returned error: %v", err)
	}
	time.Sleep(time.Millisecond)
	if _, _, err := contract.InitiateAvailabilityChallenge(contractID); err == nil {
		t.Fatal("InitiateAvailabilityChallenge accepted an expired contract")
	}
}

func TestInitiateAndRecordAvailabilityResult(t *testing.T) {
	if err := contract.ConfigureChunkCountResolver(fixedChunkCountResolver{count: 3}); err != nil {
		t.Fatalf("ConfigureChunkCountResolver returned error: %v", err)
	}
	contractID, err := contract.CreateAvailabilityContract("publisher-init", "provider-init", "file-init", 100, time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract returned error: %v", err)
	}
	if _, err := contract.FundAvailabilityContract(contractID, 100); err != nil {
		t.Fatalf("FundAvailabilityContract returned error: %v", err)
	}
	if err := contract.ConfigureChunkCountResolver(fixedChunkCountResolver{count: 7}); err != nil {
		t.Fatalf("ConfigureChunkCountResolver returned error: %v", err)
	}
	challengeID, epochID, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("InitiateAvailabilityChallenge returned error: %v", err)
	}
	if challengeID == "" || epochID == "" {
		t.Fatalf("challenge initiation returned empty IDs: %q, %q", challengeID, epochID)
	}
	if state, err := challenge.GetEpochState(epochID); err != nil || state.UncoveredChunkCount != 2 {
		t.Fatalf("epoch did not use the contract's three-chunk snapshot: state=%+v, error=%v", state, err)
	}
	state, err := contract.RecordAvailabilityResult(contractID, challengeID, availabilitytypes.ChallengeResult{Succeeded: true})
	if err != nil {
		t.Fatalf("RecordAvailabilityResult returned error: %v", err)
	}
	if state != availabilitytypes.Active {
		t.Fatalf("result state = %s, want %s", state, availabilitytypes.Active)
	}
	if _, err := contract.RecordAvailabilityResult(contractID, challengeID, availabilitytypes.ChallengeResult{Succeeded: true}); err == nil {
		t.Fatal("duplicate challenge result succeeded")
	}
	secondChallengeID, _, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("second InitiateAvailabilityChallenge returned error: %v", err)
	}
	if _, err := contract.RecordAvailabilityResult(contractID, secondChallengeID, availabilitytypes.ChallengeResult{Succeeded: false}); err != nil {
		t.Fatalf("second RecordAvailabilityResult returned error: %v", err)
	}
	status, err := contract.SettleAvailabilityContract(contractID)
	if err != nil || status != availabilitytypes.SettlementWithheld {
		t.Fatalf("partial settlement = %s, %v; want WITHHELD, nil", status, err)
	}
}
