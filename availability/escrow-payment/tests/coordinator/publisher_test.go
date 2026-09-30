package coordinator_test

import (
	"crypto/ed25519"
	"fmt"
	"testing"

	availabilitytypes "cipher/availability/availability-contracts/types"
	bindings "cipher/availability/escrow-payment/bindings"
	coordinator "cipher/availability/escrow-payment/coordinator"
	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

type recordingSubmitter struct {
	contractID string
	state      payment.PaymentState
	failedID   string
	penalty    uint64
}

func (s *recordingSubmitter) SubmitPaymentState(contractID string, state payment.PaymentState) error {
	s.contractID = contractID
	s.state = state
	return nil
}

func (s *recordingSubmitter) MarkFailure(contractID string) error {
	s.failedID = contractID
	return nil
}

func (s *recordingSubmitter) SlashCollateral(contractID string, penalty uint64) error {
	if contractID != s.failedID {
		return fmt.Errorf("slash contract %q was not marked failed", contractID)
	}
	s.penalty = penalty
	return nil
}

func TestPublisherCoordinatorProcessesAvailabilityResult(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	submitter := &recordingSubmitter{}
	workflow, err := coordinator.NewPublisherCoordinator("publisher-node", payment.Ed25519Signer{PrivateKey: privateKey}, submitter)
	if err != nil {
		t.Fatalf("NewPublisherCoordinator returned error: %v", err)
	}
	providerAddress, err := bindings.ParseEthereumAddress("0x1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("ParseEthereumAddress returned error: %v", err)
	}
	if err := workflow.RegisterProvider("peer-provider", providerAddress); err != nil {
		t.Fatalf("RegisterProvider returned error: %v", err)
	}
	escrowID, err := bindings.ParseEscrowContractID("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("ParseEscrowContractID returned error: %v", err)
	}
	if err := workflow.RegisterContract("availability-contract", escrowID); err != nil {
		t.Fatalf("RegisterContract returned error: %v", err)
	}
	state, err := workflow.ProcessAvailabilityResult(interfaces.AvailabilityResult{
		ContractID: availabilitytypes.ContractID("availability-contract"), ProviderID: "peer-provider",
		Period: 1, ChallengeID: availabilitytypes.ChallengeID("challenge-1"), Result: interfaces.AvailabilityPass,
	}, 25, 1)
	if err != nil {
		t.Fatalf("ProcessAvailabilityResult returned error: %v", err)
	}
	if state.ContractID != escrowID.String() || state.Provider != providerAddress.String() || submitter.contractID != escrowID.String() {
		t.Fatalf("mapped payment state = %+v, submitted contract = %q", state, submitter.contractID)
	}
	workflow.SetFailurePenalty(7)
	failed, err := workflow.ProcessAvailabilityResult(interfaces.AvailabilityResult{ContractID: "availability-contract", ProviderID: "peer-provider", ChallengeID: "challenge-2", Result: interfaces.AvailabilityFail}, 25, 2)
	if err != nil {
		t.Fatalf("ProcessAvailabilityResult failure path returned error: %v", err)
	}
	if failed.ContractID != "" || submitter.failedID != escrowID.String() || submitter.penalty != 7 {
		t.Fatalf("failure action = state %+v, contract %q, penalty %d", failed, submitter.failedID, submitter.penalty)
	}
}
