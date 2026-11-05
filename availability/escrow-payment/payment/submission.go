package payment

import "errors"

// EscrowSubmitter is the minimal adapter to an escrow-chain client.
type EscrowSubmitter interface {
	SubmitPaymentState(contractID string, paymentState PaymentState) error
}

// EscrowSettler is the on-chain settlement interface for the optimistic payment mechanism.
// In normal operation, vouchers are accumulated off-chain and settlement is performed
// once at the end of the contract term.
type EscrowSettler interface {
	SettleContract(contractID string, paymentState PaymentState) error
}

// EscrowFailureHandler records an Availability failure on the configured
// escrow agreement and handles collateral slashing during disputes.
type EscrowFailureHandler interface {
	MarkFailure(contractID string) error
	SlashCollateral(contractID string, penalty uint64) error
}

// PaymentStateStore keeps the latest signed cumulative payment state.
// Its zero value is ready to use and never accepts a stale sequence number.
type PaymentStateStore struct {
	Latest   PaymentState
	HasState bool
}

// StorePaymentState stores a newer signed payment state.
//
// Monotonic invariants enforced:
//   - Sequence must be strictly increasing (no replays).
//   - CumulativePayment must be non-decreasing (payments only grow).
//   - ContractID must match the first stored state (no cross-contract confusion).
func (store *PaymentStateStore) StorePaymentState(paymentState PaymentState) error {
	if paymentState.ContractID == "" || len(paymentState.PublisherSignature) == 0 {
		return errors.New("a signed payment state is required")
	}

	if store.HasState &&
		(paymentState.ContractID != store.Latest.ContractID ||
			paymentState.Sequence <= store.Latest.Sequence ||
			paymentState.CumulativePayment < store.Latest.CumulativePayment) {
		return errors.New("payment state is stale or belongs to another contract")
	}

	store.Latest = paymentState
	store.HasState = true

	return nil
}

// GetLatestPaymentState returns the latest stored signed payment state.
func (store *PaymentStateStore) GetLatestPaymentState() (PaymentState, error) {
	if !store.HasState {
		return PaymentState{}, errors.New("no payment state is stored")
	}

	return store.Latest, nil
}

// SubmitPaymentState passes a signed payment state to a configured escrow
// adapter. The package does not embed blockchain-client details.
func SubmitPaymentState(
	contractID string,
	paymentState PaymentState,
	submitter EscrowSubmitter,
) error {

	if contractID == "" ||
		paymentState.ContractID != contractID ||
		submitter == nil {
		return errors.New("matching contract ID and escrow submitter are required")
	}

	if len(paymentState.PublisherSignature) == 0 {
		return errors.New("payment state must be signed before submission")
	}

	return submitter.SubmitPaymentState(contractID, paymentState)
}
