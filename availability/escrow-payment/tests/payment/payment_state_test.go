package payment_test

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

type recordingSubmitter struct {
	contractID string
	state      payment.PaymentState
	err        error
}

func (s *recordingSubmitter) SubmitPaymentState(contractID string, state payment.PaymentState) error {
	s.contractID = contractID
	s.state = state
	return s.err
}

func TestPaymentStateLifecycle(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	result := interfaces.AvailabilityResult{
		ContractID: "contract-payment", ProviderID: "provider-payment", Period: 1,
		ChallengeID: "challenge-payment", Result: interfaces.AvailabilityPass, Timestamp: time.Now(),
	}
	state, err := payment.CreatePaymentState(result, "publisher-payment", 50, 1)
	if err != nil {
		t.Fatalf("CreatePaymentState returned error: %v", err)
	}
	signed, err := payment.SignPaymentState(state, privateKey)
	if err != nil {
		t.Fatalf("SignPaymentState returned error: %v", err)
	}
	if !payment.VerifyPaymentState(signed, publicKey) {
		t.Fatal("VerifyPaymentState rejected a valid signed state")
	}
	signed.CumulativePayment = 51
	if payment.VerifyPaymentState(signed, publicKey) {
		t.Fatal("VerifyPaymentState accepted a modified state")
	}
}

func TestPaymentStoreAndSubmission(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	result := interfaces.AvailabilityResult{ContractID: "contract-store", ProviderID: "provider-store", ChallengeID: "challenge-store", Result: interfaces.AvailabilityPass}
	state, err := payment.CreatePaymentState(result, "publisher-store", 20, 1)
	if err != nil {
		t.Fatalf("CreatePaymentState returned error: %v", err)
	}
	signed, err := payment.SignPaymentState(state, privateKey)
	if err != nil {
		t.Fatalf("SignPaymentState returned error: %v", err)
	}
	var store payment.PaymentStateStore
	if err := store.StorePaymentState(signed); err != nil {
		t.Fatalf("StorePaymentState returned error: %v", err)
	}
	if err := store.StorePaymentState(signed); err == nil {
		t.Fatal("StorePaymentState accepted a stale sequence")
	}

	// Test Bug 6 fix: Reject non-monotonic cumulative payment (even if sequence is higher).
	decreasedCumState := signed
	decreasedCumState.Sequence = signed.Sequence + 1
	decreasedCumState.CumulativePayment = signed.CumulativePayment - 1
	if err := store.StorePaymentState(decreasedCumState); err == nil {
		t.Fatal("StorePaymentState accepted a decreased cumulative payment")
	}
	submitter := &recordingSubmitter{}
	if err := payment.SubmitPaymentState(signed.ContractID, signed, submitter); err != nil {
		t.Fatalf("SubmitPaymentState returned error: %v", err)
	}
	if submitter.contractID != signed.ContractID || submitter.state.Sequence != signed.Sequence {
		t.Fatal("escrow adapter did not receive the payment state")
	}
	submitter.err = errors.New("escrow rejected state")
	if err := payment.SubmitPaymentState(signed.ContractID, signed, submitter); !errors.Is(err, submitter.err) {
		t.Fatalf("SubmitPaymentState error = %v, want adapter error", err)
	}
}

func TestCreatePaymentStateRejectsFailure(t *testing.T) {
	result := interfaces.AvailabilityResult{ContractID: "contract-fail", ProviderID: "provider-fail", ChallengeID: "challenge-fail", Result: interfaces.AvailabilityFail}
	if _, err := payment.CreatePaymentState(result, "publisher-fail", 1, 1); err == nil {
		t.Fatal("CreatePaymentState accepted a failed availability result")
	}
}

func TestPaymentStoreRejectsDifferentContractAndUnsignedSubmission(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	firstResult := interfaces.AvailabilityResult{ContractID: "contract-one", ProviderID: "provider", ChallengeID: "challenge-one", Result: interfaces.AvailabilityPass}
	first, err := payment.CreatePaymentState(firstResult, "publisher", 10, 1)
	if err != nil {
		t.Fatalf("CreatePaymentState returned error: %v", err)
	}
	first, err = payment.SignPaymentState(first, privateKey)
	if err != nil {
		t.Fatalf("SignPaymentState returned error: %v", err)
	}
	var store payment.PaymentStateStore
	if err := store.StorePaymentState(first); err != nil {
		t.Fatalf("StorePaymentState returned error: %v", err)
	}
	secondResult := interfaces.AvailabilityResult{ContractID: "contract-two", ProviderID: "provider", ChallengeID: "challenge-two", Result: interfaces.AvailabilityPass}
	second, err := payment.CreatePaymentState(secondResult, "publisher", 20, 2)
	if err != nil {
		t.Fatalf("CreatePaymentState returned error: %v", err)
	}
	second, err = payment.SignPaymentState(second, privateKey)
	if err != nil {
		t.Fatalf("SignPaymentState returned error: %v", err)
	}
	if err := store.StorePaymentState(second); err == nil {
		t.Fatal("StorePaymentState accepted a state for a different contract")
	}
	if err := payment.SubmitPaymentState(first.ContractID, payment.PaymentState{ContractID: first.ContractID}, &recordingSubmitter{}); err == nil {
		t.Fatal("SubmitPaymentState accepted an unsigned state")
	}
}
