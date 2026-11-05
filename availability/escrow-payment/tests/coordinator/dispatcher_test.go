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

func TestAvailabilityPaymentDispatcher_PassSequenceAndCap(t *testing.T) {
	privKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	submitter := &recordingSubmitter{}

	pubCoordinator, err := coordinator.NewPublisherCoordinator(
		"publisher-node",
		payment.Ed25519Signer{PrivateKey: privKey},
		submitter,
	)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	providerAddr, err := bindings.ParseEthereumAddress(
		"0x2222222222222222222222222222222222222222",
	)
	if err != nil {
		t.Fatalf("failed to parse provider address: %v", err)
	}

	if err := pubCoordinator.RegisterProvider(
		"peer-provider-1",
		providerAddr,
	); err != nil {
		t.Fatalf("failed to register provider: %v", err)
	}

	escrowID, err := bindings.ParseEscrowContractID(
		"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)
	if err != nil {
		t.Fatalf("failed to parse escrow ID: %v", err)
	}

	dispatcher, err := coordinator.NewAvailabilityPaymentDispatcher(
		pubCoordinator,
		privKey,
	)
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	availContractID := availabilitytypes.ContractID("contract-flow-1")

	// 25 per PASS, maximum cumulative payment = 60.
	if err := dispatcher.RegisterContractSchedule(
		availContractID,
		escrowID,
		25,
		60,
	); err != nil {
		t.Fatalf("failed to register contract schedule: %v", err)
	}

	// ------------------------------------------------------------
	// 1. First PASS
	// cumulative = 25, sequence = 1
	// ------------------------------------------------------------

	res1 := interfaces.AvailabilityResult{
		ContractID:  availContractID,
		ProviderID:  "peer-provider-1",
		Period:      1,
		ChallengeID: availabilitytypes.ChallengeID("chal-1"),
		Result:      interfaces.AvailabilityPass,
	}

	state1, err := dispatcher.DispatchAvailabilityResult(res1)
	if err != nil {
		t.Fatalf("failed to dispatch res1: %v", err)
	}

	if state1.Kind != coordinator.DispatchKindPass {
		t.Fatalf(
			"expected PASS dispatch kind, got %v",
			state1.Kind,
		)
	}

	if state1.PaymentState.Sequence != 1 ||
		state1.PaymentState.CumulativePayment != 25 {
		t.Fatalf(
			"expected seq 1, cumPayment 25; got seq %d, cum %d",
			state1.PaymentState.Sequence,
			state1.PaymentState.CumulativePayment,
		)
	}

	// ------------------------------------------------------------
	// 2. Second PASS
	// cumulative = 50, sequence = 2
	// ------------------------------------------------------------

	res2 := interfaces.AvailabilityResult{
		ContractID:  availContractID,
		ProviderID:  "peer-provider-1",
		Period:      2,
		ChallengeID: availabilitytypes.ChallengeID("chal-2"),
		Result:      interfaces.AvailabilityPass,
	}

	state2, err := dispatcher.DispatchAvailabilityResult(res2)
	if err != nil {
		t.Fatalf("failed to dispatch res2: %v", err)
	}

	if state2.Kind != coordinator.DispatchKindPass {
		t.Fatalf(
			"expected PASS dispatch kind, got %v",
			state2.Kind,
		)
	}

	if state2.PaymentState.Sequence != 2 ||
		state2.PaymentState.CumulativePayment != 50 {
		t.Fatalf(
			"expected seq 2, cumPayment 50; got seq %d, cum %d",
			state2.PaymentState.Sequence,
			state2.PaymentState.CumulativePayment,
		)
	}

	// ------------------------------------------------------------
	// 3. Third PASS
	// cumulative would be 75, but is capped at 60.
	// sequence = 3
	// ------------------------------------------------------------

	res3 := interfaces.AvailabilityResult{
		ContractID:  availContractID,
		ProviderID:  "peer-provider-1",
		Period:      3,
		ChallengeID: availabilitytypes.ChallengeID("chal-3"),
		Result:      interfaces.AvailabilityPass,
	}

	state3, err := dispatcher.DispatchAvailabilityResult(res3)
	if err != nil {
		t.Fatalf("failed to dispatch res3: %v", err)
	}

	if state3.Kind != coordinator.DispatchKindPass {
		t.Fatalf(
			"expected PASS dispatch kind, got %v",
			state3.Kind,
		)
	}

	if state3.PaymentState.Sequence != 3 ||
		state3.PaymentState.CumulativePayment != 60 {
		t.Fatalf(
			"expected seq 3, capped cumPayment 60; got seq %d, cum %d",
			state3.PaymentState.Sequence,
			state3.PaymentState.CumulativePayment,
		)
	}

	// ------------------------------------------------------------
	// 4. Duplicate challenge must be rejected.
	// ------------------------------------------------------------

	_, err = dispatcher.DispatchAvailabilityResult(res3)
	if err == nil {
		t.Fatalf(
			"expected error when dispatching duplicate challenge result, got nil",
		)
	}

	// ------------------------------------------------------------
	// 5. Test SubmitSettlementOnChain (Bug 2 fix).
	// ------------------------------------------------------------
	pubCoordinator.SetSettler(submitter)
	if err := dispatcher.SubmitSettlementOnChain(availContractID); err != nil {
		t.Fatalf("SubmitSettlementOnChain returned error: %v", err)
	}

	if submitter.settledContractID != escrowID.String() {
		t.Fatalf("expected settled contract ID %q, got %q", escrowID.String(), submitter.settledContractID)
	}
	if submitter.settledState.Sequence != 3 || submitter.settledState.CumulativePayment != 60 {
		t.Fatalf("expected settled state seq 3, cum 60; got seq %d, cum %d",
			submitter.settledState.Sequence, submitter.settledState.CumulativePayment)
	}
}

func TestAvailabilityPaymentDispatcher_FailStreak(t *testing.T) {
	privKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	submitter := &recordingSubmitter{}

	pubCoordinator, err := coordinator.NewPublisherCoordinator(
		"publisher-node",
		payment.Ed25519Signer{PrivateKey: privKey},
		submitter,
	)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	providerAddr, err := bindings.ParseEthereumAddress(
		"0x3333333333333333333333333333333333333333",
	)
	if err != nil {
		t.Fatalf("failed to parse provider address: %v", err)
	}

	if err := pubCoordinator.RegisterProvider(
		"peer-provider-fail",
		providerAddr,
	); err != nil {
		t.Fatalf("failed to register provider: %v", err)
	}

	escrowID, err := bindings.ParseEscrowContractID(
		"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	)
	if err != nil {
		t.Fatalf("failed to parse escrow ID: %v", err)
	}

	dispatcher, err := coordinator.NewAvailabilityPaymentDispatcher(
		pubCoordinator,
		privKey,
	)
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	availContractID := availabilitytypes.ContractID(
		"contract-fail-test",
	)

	if err := dispatcher.RegisterContractSchedule(
		availContractID,
		escrowID,
		50,
		200,
	); err != nil {
		t.Fatalf("failed to register contract schedule: %v", err)
	}

	// ------------------------------------------------------------
	// 1. First FAIL
	// ------------------------------------------------------------

	failRes1 := interfaces.AvailabilityResult{
		ContractID:  availContractID,
		ProviderID:  "peer-provider-fail",
		Period:      1,
		ChallengeID: availabilitytypes.ChallengeID("chal-fail-1"),
		Result:      interfaces.AvailabilityFail,
	}

	failedState1, err := dispatcher.DispatchAvailabilityResult(failRes1)
	if err != nil {
		t.Fatalf("unexpected error on first fail: %v", err)
	}

	if failedState1.Kind != coordinator.DispatchKindFail {
		t.Fatalf(
			"expected FAIL dispatch kind, got %v",
			failedState1.Kind,
		)
	}

	// FAIL must not create a PaymentState.
	if failedState1.PaymentState.ContractID != "" {
		t.Fatalf(
			"expected empty payment state on FAIL, got %+v",
			failedState1.PaymentState,
		)
	}

	if failedState1.ConsecutiveFailures != 1 {
		t.Fatalf(
			"expected consecutive failures = 1, got %d",
			failedState1.ConsecutiveFailures,
		)
	}

	if failedState1.SlashEligible {
		t.Fatalf("first failure must not be slash-eligible")
	}

	if len(failedState1.SlashEvidence) != 0 {
		t.Fatalf(
			"expected no slash evidence before threshold, got %d records",
			len(failedState1.SlashEvidence),
		)
	}

	// Dispatcher must not submit anything on-chain for a FAIL.
	if submitter.contractID != "" {
		t.Fatalf(
			"dispatcher must not submit failure on-chain; got contractID %s",
			submitter.contractID,
		)
	}

	// ------------------------------------------------------------
	// 2. Second consecutive FAIL
	// ------------------------------------------------------------

	failRes2 := interfaces.AvailabilityResult{
		ContractID:  availContractID,
		ProviderID:  "peer-provider-fail",
		Period:      2,
		ChallengeID: availabilitytypes.ChallengeID("chal-fail-2"),
		Result:      interfaces.AvailabilityFail,
	}

	failedState2, err := dispatcher.DispatchAvailabilityResult(failRes2)
	if err != nil {
		t.Fatalf("unexpected error on second fail: %v", err)
	}

	if failedState2.Kind != coordinator.DispatchKindFail {
		t.Fatalf(
			"expected FAIL dispatch kind, got %v",
			failedState2.Kind,
		)
	}

	if failedState2.ConsecutiveFailures != 2 {
		t.Fatalf(
			"expected consecutive failures = 2, got %d",
			failedState2.ConsecutiveFailures,
		)
	}

	if failedState2.SlashEligible {
		t.Fatalf("second failure must not be slash-eligible")
	}

	if len(failedState2.SlashEvidence) != 0 {
		t.Fatalf(
			"expected no slash evidence before threshold, got %d records",
			len(failedState2.SlashEvidence),
		)
	}

	// ------------------------------------------------------------
	// 3. Third consecutive FAIL
	// ------------------------------------------------------------

	failRes3 := interfaces.AvailabilityResult{
		ContractID:  availContractID,
		ProviderID:  "peer-provider-fail",
		Period:      3,
		ChallengeID: availabilitytypes.ChallengeID("chal-fail-3"),
		Result:      interfaces.AvailabilityFail,
	}

	failedState3, err := dispatcher.DispatchAvailabilityResult(failRes3)
	if err != nil {
		t.Fatalf("unexpected error on third fail: %v", err)
	}

	if failedState3.Kind != coordinator.DispatchKindFail {
		t.Fatalf(
			"expected FAIL dispatch kind, got %v",
			failedState3.Kind,
		)
	}

	if failedState3.ConsecutiveFailures != 3 {
		t.Fatalf(
			"expected consecutive failures = 3, got %d",
			failedState3.ConsecutiveFailures,
		)
	}

	// Default threshold = 3.
	if !failedState3.SlashEligible {
		t.Fatalf(
			"expected slash eligibility after 3 consecutive failures",
		)
	}

	if len(failedState3.SlashEvidence) != 3 {
		t.Fatalf(
			"expected 3 slash evidence records, got %d",
			len(failedState3.SlashEvidence),
		)
	}

	// ------------------------------------------------------------
	// 4. Verify evidence
	// ------------------------------------------------------------

	for i, record := range failedState3.SlashEvidence {
		expectedSequence := uint64(i + 1)

		if record.Sequence != expectedSequence {
			t.Fatalf(
				"expected evidence[%d] sequence %d, got %d",
				i,
				expectedSequence,
				record.Sequence,
			)
		}

		if record.EscrowContractID != escrowID.String() {
			t.Fatalf(
				"expected evidence[%d] escrow ID %s, got %s",
				i,
				escrowID.String(),
				record.EscrowContractID,
			)
		}

		if record.ProviderEthAddress != providerAddr.String() {
			t.Fatalf(
				"expected evidence[%d] provider %s, got %s",
				i,
				providerAddr.String(),
				record.ProviderEthAddress,
			)
		}

		if record.ChallengeID == "" {
			t.Fatalf(
				"expected evidence[%d] to contain challenge ID",
				i,
			)
		}

		if record.IssuedAt.IsZero() {
			t.Fatalf(
				"expected evidence[%d] to contain issued timestamp",
				i,
			)
		}

		if len(record.PublisherSignature) == 0 {
			t.Fatalf(
				"expected evidence[%d] to contain publisher signature",
				i,
			)
		}
	}

	// ------------------------------------------------------------
	// 5. Verify the complete fail streak cryptographically.
	// VerifyFailStreak returns a single bool.
	// ------------------------------------------------------------

	if !coordinator.VerifyFailStreak(
		failedState3.SlashEvidence,
		privKey.Public().(ed25519.PublicKey),
	) {
		t.Fatalf(
			"expected fail streak signature verification to succeed",
		)
	}

	// ------------------------------------------------------------
	// 6. Confirm no on-chain failure/slashing happened.
	// ------------------------------------------------------------

	if submitter.contractID != "" {
		t.Fatalf(
			"dispatcher must not submit failure on-chain; got contractID %s",
			submitter.contractID,
		)
	}
}