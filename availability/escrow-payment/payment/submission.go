package payment

import "errors"

// EscrowSubmitter is the minimal adapter to an escrow-chain client.
type EscrowSubmitter interface {
	SubmitPaymentState(contractID string, paymentState PaymentState) error
}

// PaymentStateStore keeps the provider's latest signed cumulative state. Its
// zero value is ready to use and never accepts a stale sequence number.
type PaymentStateStore struct {
	Latest   PaymentState
	HasState bool
}

func (store *PaymentStateStore) StorePaymentState(paymentState PaymentState) error {
	if paymentState.ContractID == "" || len(paymentState.PublisherSignature) == 0 {
		return errors.New("a signed payment state is required")
	}
	if store.HasState && (paymentState.ContractID != store.Latest.ContractID || paymentState.Sequence <= store.Latest.Sequence) {
		return errors.New("payment state is stale or belongs to another contract")
	}
	store.Latest = paymentState
	store.HasState = true
	return nil
}

// SubmitPaymentState passes a signed payment state to a configured escrow
// adapter. The package does not embed blockchain-client details.
func SubmitPaymentState(contractID string, paymentState PaymentState, submitter EscrowSubmitter) error {
	if contractID == "" || paymentState.ContractID != contractID || submitter == nil {
		return errors.New("matching contract ID and escrow submitter are required")
	}
	if len(paymentState.PublisherSignature) == 0 {
		return errors.New("payment state must be signed before submission")
	}
	return submitter.SubmitPaymentState(contractID, paymentState)
}
