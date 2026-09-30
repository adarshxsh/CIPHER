package coordinator_test

import (
	"crypto/ed25519"
	"testing"

	availabilitytypes "cipher/availability/availability-contracts/types"
	coordinator "cipher/availability/escrow-payment/coordinator"
	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

type recordingSubmitter struct {
	contractID string
	state      payment.PaymentState
}

func (s *recordingSubmitter) SubmitPaymentState(contractID string, state payment.PaymentState) error {
	s.contractID = contractID
	s.state = state
	return nil
}

func TestPublisherCoordinatorProcessesAvailabilityResult(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	submitter := &recordingSubmitter{}
	workflow, err := coordinator.NewPublisherCoordinator("publisher-node", payment.Ed25519Signer{PrivateKey: privateKey}, submitter)
	if err != nil {
		t.Fatalf("NewPublisherCoordinator returned error: %v", err)
	}
	if err := workflow.RegisterProvider("peer-provider", "payment-provider"); err != nil {
		t.Fatalf("RegisterProvider returned error: %v", err)
	}
	if err := workflow.RegisterContract("availability-contract", "escrow-contract"); err != nil {
		t.Fatalf("RegisterContract returned error: %v", err)
	}
	state, err := workflow.ProcessAvailabilityResult(interfaces.AvailabilityResult{
		ContractID: availabilitytypes.ContractID("availability-contract"), ProviderID: "peer-provider",
		Period: 1, ChallengeID: availabilitytypes.ChallengeID("challenge-1"), Result: interfaces.AvailabilityPass,
	}, 25, 1)
	if err != nil {
		t.Fatalf("ProcessAvailabilityResult returned error: %v", err)
	}
	if state.ContractID != "escrow-contract" || state.Provider != "payment-provider" || submitter.contractID != "escrow-contract" {
		t.Fatalf("mapped payment state = %+v, submitted contract = %q", state, submitter.contractID)
	}
	if _, err := workflow.ProcessAvailabilityResult(interfaces.AvailabilityResult{ContractID: "availability-contract", ProviderID: "peer-provider", ChallengeID: "challenge-2", Result: interfaces.AvailabilityFail}, 25, 2); err == nil {
		t.Fatal("ProcessAvailabilityResult accepted a failed availability result")
	}
}
