package integration_test

import (
	"testing"
	"time"

	contract "cipher/availability/availability-contracts/contract"
	availabilitytypes "cipher/availability/availability-contracts/types"
)

type integrationChunkCountResolver struct{}

func (integrationChunkCountResolver) ChunkCount(providerID, fileID string) (int, error) {
	return 4, nil
}

func TestAvailabilityFlow(t *testing.T) {
	if err := contract.ConfigureChunkCountResolver(integrationChunkCountResolver{}); err != nil {
		t.Fatalf("ConfigureChunkCountResolver returned error: %v", err)
	}
	contractID, err := contract.CreateAvailabilityContract("publisher-flow", "provider-flow", "file-flow", 120, time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract returned error: %v", err)
	}
	if _, err := contract.FundAvailabilityContract(contractID, 120); err != nil {
		t.Fatalf("FundAvailabilityContract returned error: %v", err)
	}
	challengeID, epochID, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil || challengeID == "" || epochID == "" {
		t.Fatalf("InitiateAvailabilityChallenge = %q, %q, %v", challengeID, epochID, err)
	}
	if _, err := contract.RecordAvailabilityResult(contractID, challengeID, availabilitytypes.ChallengeResult{Succeeded: true}); err != nil {
		t.Fatalf("RecordAvailabilityResult returned error: %v", err)
	}
	status, err := contract.SettleAvailabilityContract(contractID)
	if err != nil {
		t.Fatalf("SettleAvailabilityContract returned error: %v", err)
	}
	if status != availabilitytypes.SettlementCompleted {
		t.Fatalf("settlement status = %s, want %s", status, availabilitytypes.SettlementCompleted)
	}
}
