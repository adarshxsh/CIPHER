package coordinator_test

import (
	"crypto/ed25519"
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

	settledContractID string
	settledState      payment.PaymentState
}

func (s *recordingSubmitter) SubmitPaymentState(
	contractID string,
	state payment.PaymentState,
) error {
	s.contractID = contractID
	s.state = state
	return nil
}

func (s *recordingSubmitter) SettleContract(
	contractID string,
	state payment.PaymentState,
) error {
	s.settledContractID = contractID
	s.settledState = state
	return nil
}

func TestPublisherCoordinatorCreatesAndStoresPaymentState(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(
		make([]byte, ed25519.SeedSize),
	)

	submitter := &recordingSubmitter{}

	workflow, err := coordinator.NewPublisherCoordinator(
		"publisher-node",
		payment.Ed25519Signer{
			PrivateKey: privateKey,
		},
		submitter,
	)
	if err != nil {
		t.Fatalf(
			"NewPublisherCoordinator returned error: %v",
			err,
		)
	}

	providerAddress, err := bindings.ParseEthereumAddress(
		"0x1111111111111111111111111111111111111111",
	)
	if err != nil {
		t.Fatalf(
			"ParseEthereumAddress returned error: %v",
			err,
		)
	}

	if err := workflow.RegisterProvider(
		"peer-provider",
		providerAddress,
	); err != nil {
		t.Fatalf(
			"RegisterProvider returned error: %v",
			err,
		)
	}

	escrowID, err := bindings.ParseEscrowContractID(
		"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	if err != nil {
		t.Fatalf(
			"ParseEscrowContractID returned error: %v",
			err,
		)
	}

	if err := workflow.RegisterContract(
		"availability-contract",
		escrowID,
	); err != nil {
		t.Fatalf(
			"RegisterContract returned error: %v",
			err,
		)
	}

	// PASS is created and signed off-chain.
	state, err := workflow.CreateAndSignPaymentState(
		interfaces.AvailabilityResult{
			ContractID:  availabilitytypes.ContractID("availability-contract"),
			ProviderID:  "peer-provider",
			Period:      1,
			ChallengeID: availabilitytypes.ChallengeID("challenge-1"),
			Result:      interfaces.AvailabilityPass,
		},
		25,
		1,
	)
	if err != nil {
		t.Fatalf(
			"CreateAndSignPaymentState returned error: %v",
			err,
		)
	}

	// Verify the availability identity was mapped to the escrow identity.
	if state.ContractID != escrowID.String() {
		t.Fatalf(
			"expected escrow contract ID %q, got %q",
			escrowID.String(),
			state.ContractID,
		)
	}

	if state.Provider != providerAddress.String() {
		t.Fatalf(
			"expected provider %q, got %q",
			providerAddress.String(),
			state.Provider,
		)
	}

	if state.CumulativePayment != 25 {
		t.Fatalf(
			"expected cumulative payment 25, got %d",
			state.CumulativePayment,
		)
	}

	if state.Sequence != 1 {
		t.Fatalf(
			"expected sequence 1, got %d",
			state.Sequence,
		)
	}

	if len(state.PublisherSignature) == 0 {
		t.Fatal("expected payment state to contain publisher signature")
	}

	// Creating/signing a payment state must NOT submit it on-chain.
	if submitter.contractID != "" {
		t.Fatalf(
			"payment state was submitted during signing; got contract %q",
			submitter.contractID,
		)
	}

	// Explicit settlement step.
	if err := workflow.SubmitLatestPaymentStateOnChain(
		escrowID.String(),
	); err != nil {
		t.Fatalf(
			"SubmitLatestPaymentStateOnChain returned error: %v",
			err,
		)
	}

	// Now the submitter should have received the stored latest state.
	if submitter.contractID != escrowID.String() {
		t.Fatalf(
			"expected submitted contract %q, got %q",
			escrowID.String(),
			submitter.contractID,
		)
	}

	if submitter.state.ContractID != escrowID.String() {
		t.Fatalf(
			"submitted state contract ID = %q, expected %q",
			submitter.state.ContractID,
			escrowID.String(),
		)
	}

	if submitter.state.Sequence != 1 {
		t.Fatalf(
			"submitted state sequence = %d, expected 1",
			submitter.state.Sequence,
		)
	}

	if submitter.state.CumulativePayment != 25 {
		t.Fatalf(
			"submitted cumulative payment = %d, expected 25",
			submitter.state.CumulativePayment,
		)
	}

	// Verify two-step settlement flow (Bug 2 fix):
	// SettleContractOnChain submits the latest state and triggers settleContract.
	workflow.SetSettler(submitter)
	if err := workflow.SettleContractOnChain(escrowID.String()); err != nil {
		t.Fatalf("SettleContractOnChain returned error: %v", err)
	}

	if submitter.settledContractID != escrowID.String() {
		t.Fatalf("expected settled contract ID %q, got %q", escrowID.String(), submitter.settledContractID)
	}
	if submitter.settledState.Sequence != 1 || submitter.settledState.CumulativePayment != 25 {
		t.Fatalf("expected settled state seq 1, cum 25, got seq %d, cum %d",
			submitter.settledState.Sequence, submitter.settledState.CumulativePayment)
	}
}