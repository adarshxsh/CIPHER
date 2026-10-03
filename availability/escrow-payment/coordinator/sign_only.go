// Package coordinator provides the temporary publisher-run workflow layer.
//
// This file (sign_only.go) extends PublisherCoordinator with a signing-only
// method that produces an authorized PaymentState WITHOUT submitting it on-chain.
//
// In the optimistic off-chain model:
//   - The publisher signs PaymentStates and sends them directly to the provider via P2P.
//   - The provider accumulates signed PaymentStates as off-chain redemption vouchers.
//   - No on-chain transactions occur during normal operation.
//   - On-chain interaction happens ONLY at:
//     1. Contract settlement (provider submits the latest PaymentState to claim funds).
//     2. Dispute / slashing (see dispatcher.go for the K-consecutive-failure rule).
//
// REQUIRED CHANGE IN publisher.go
// ===============================
// PublisherCoordinator needs one new field so tests can control time:
//
//	now func() time.Time // optional; nil means time.Now
//
// ABOUT paymentStateTTL
// =====================
// ValidUntil is the voucher's REDEMPTION window, not the challenge deadline.
// A voucher is only signed after a verified PASS, so the challenge deadline has
// already been enforced. paymentStateTTL must therefore be longer than
// (contract duration + dispute window + settlement grace), otherwise the latest
// voucher can expire before the provider settles and an honest provider loses
// earned funds. (A later iteration may derive ValidUntil from the contract end.)
package coordinator

import (
	"errors"
	"fmt"
	"time"

	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

// Sentinel errors for the signing path. Wrapped with %w so callers and tests
// can use errors.Is.
var (
	ErrNotPassResult     = errors.New("only PASS results produce a payment state")
	ErrNoEscrowBinding   = errors.New("no escrow contract is registered for this availability contract")
	ErrNoProviderAddress = errors.New("no Ethereum address registered for this provider")
	ErrInvalidTTL        = errors.New("payment state TTL must be positive")
	ErrNoStore           = errors.New("no payment state store for this escrow contract")
	ErrStaleState        = errors.New("payment state is not newer than the stored latest state")
	ErrNoStoredState     = errors.New("no stored payment state available for this escrow contract")
	ErrStateExpired      = errors.New("latest stored payment state has expired")
)

// currentTime returns the coordinator clock (injectable for tests).
func (c *PublisherCoordinator) currentTime() time.Time {
	if c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}

// ProviderAddressFor resolves the provider's Ethereum address (as a string) for
// a result, taking the coordinator lock. Other components (e.g. the dispatcher)
// must use this instead of reading c.bindings directly, which would race with
// RegisterContract.
func (c *PublisherCoordinator) ProviderAddressFor(
	result interfaces.AvailabilityResult,
) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	addr, ok := c.bindings.ProviderAddress(result.ProviderID)
	if !ok {
		return "", false
	}
	return addr.String(), true
}

// CreateAndSignPaymentState creates an authorized, signed off-chain PaymentState
// for a verified PASS result.
//
// It does NOT submit anything on-chain. The returned signed PaymentState is
// delivered to the provider via P2P as an off-chain redemption voucher.
//
// The latest-state store only ever moves forward: a state whose sequence is not
// strictly greater, or whose cumulative payment is lower, than the stored latest
// is rejected with ErrStaleState. The store is written last, so on any error the
// coordinator state is unchanged.
func (c *PublisherCoordinator) CreateAndSignPaymentState(
	result interfaces.AvailabilityResult,
	cumulativePayment uint64,
	sequence uint64,
) (payment.PaymentState, error) {
	if result.Result != interfaces.AvailabilityPass {
		return payment.PaymentState{}, ErrNotPassResult
	}
	if c.paymentStateTTL <= 0 {
		return payment.PaymentState{}, ErrInvalidTTL
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Resolve the on-chain escrow agreement ID for this availability contract.
	escrowContractID, ok := c.bindings.EscrowID(result.ContractID)
	if !ok {
		return payment.PaymentState{}, ErrNoEscrowBinding
	}

	// Resolve the provider's Ethereum address from their availability peer ID.
	providerAddress, ok := c.bindings.ProviderAddress(result.ProviderID)
	if !ok {
		return payment.PaymentState{}, ErrNoProviderAddress
	}

	// Look up the store BEFORE signing so we fail early and never sign a state
	// we cannot record.
	escrowKey := escrowContractID.String()
	store, ok := c.stores[escrowKey]
	if !ok {
		return payment.PaymentState{}, fmt.Errorf("%w: %s", ErrNoStore, escrowKey)
	}

	// Monotonic check (ASSUMES PaymentState has Sequence and CumulativePayment
	// fields; rename to match payment.PaymentState if they differ).
	if store.HasState {
		if sequence <= store.Latest.Sequence || cumulativePayment < store.Latest.CumulativePayment {
			return payment.PaymentState{}, fmt.Errorf(
				"%w: got seq=%d cum=%d, stored seq=%d cum=%d",
				ErrStaleState, sequence, cumulativePayment,
				store.Latest.Sequence, store.Latest.CumulativePayment,
			)
		}
	}

	// Rewrite ProviderID as its Ethereum address string so the on-chain contract
	// can validate the provider's identity. `result` is a copy, so the caller's
	// struct is untouched.
	result.ProviderID = providerAddress.String()

	state, err := payment.CreatePaymentState(result, c.publisherID, cumulativePayment, sequence)
	if err != nil {
		return payment.PaymentState{}, err
	}

	state.ContractID = escrowKey
	state.ValidUntil = uint64(c.currentTime().Add(c.paymentStateTTL).Unix())

	signed, err := c.signer.Sign(state)
	if err != nil {
		return payment.PaymentState{}, err
	}

	// Commit last. If this fails nothing has been stored and the caller may retry.
	if err := store.StorePaymentState(signed); err != nil {
		return payment.PaymentState{}, err
	}
	c.stores[escrowKey] = store

	return signed, nil
}

// SubmitLatestPaymentStateOnChain submits the stored latest PaymentState to the
// escrow contract on-chain.
//
// This is STEP 1 of the two-step settlement flow:
//   1. submitPaymentState — record the latest off-chain voucher on-chain.
//   2. settleContract     — distribute funds to provider and publisher.
//
// Called ONLY when the publisher initiates settlement, or the provider is
// unresponsive. Under normal operation providers submit their own latest state.
//
// It refuses to spend gas on a voucher that has already expired.
func (c *PublisherCoordinator) SubmitLatestPaymentStateOnChain(escrowContractIDStr string) error {
	c.mu.Lock()
	store, exists := c.stores[escrowContractIDStr]
	now := c.currentTime()
	c.mu.Unlock()

	if !exists || !store.HasState {
		return fmt.Errorf("%w: %s", ErrNoStoredState, escrowContractIDStr)
	}
	if store.Latest.ValidUntil <= uint64(now.Unix()) {
		return fmt.Errorf("%w: valid until %d, now %d",
			ErrStateExpired, store.Latest.ValidUntil, now.Unix())
	}

	// The intentional on-chain transaction; gas is spent only once at settlement.
	return payment.SubmitPaymentState(escrowContractIDStr, store.Latest, c.submitter)
}

// SetSettler sets the on-chain settlement adapter. This is optional because
// not all callers (e.g. unit tests with a recording submitter) need to perform
// the full settlement flow.
func (c *PublisherCoordinator) SetSettler(settler payment.EscrowSettler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.settler = settler
}

// SettleContractOnChain performs the complete two-step settlement flow:
//
//  1. Submit the latest stored PaymentState on-chain (submitPaymentState).
//  2. Call settleContract to distribute funds (provider gets cumulativePayment,
//     publisher gets remaining escrow balance).
//
// This is the ONLY method that actually moves ETH. In the optimistic model,
// it is called once at the end of the contract term — not during individual
// challenge rounds.
//
// State transitions on-chain:
//
//	Active → (voucher recorded) → Settled
//	Provider receives: cumulativePayment
//	Publisher receives: reward − cumulativePayment
func (c *PublisherCoordinator) SettleContractOnChain(escrowContractIDStr string) error {
	c.mu.Lock()
	store, exists := c.stores[escrowContractIDStr]
	now := c.currentTime()
	settler := c.settler
	c.mu.Unlock()

	if settler == nil {
		return errors.New("no settler configured; call SetSettler before settlement")
	}
	if !exists || !store.HasState {
		return fmt.Errorf("%w: %s", ErrNoStoredState, escrowContractIDStr)
	}
	if store.Latest.ValidUntil <= uint64(now.Unix()) {
		return fmt.Errorf("%w: valid until %d, now %d",
			ErrStateExpired, store.Latest.ValidUntil, now.Unix())
	}

	// Step 1: Record the latest off-chain voucher on-chain.
	if err := payment.SubmitPaymentState(escrowContractIDStr, store.Latest, c.submitter); err != nil {
		return fmt.Errorf("submit payment state on-chain: %w", err)
	}

	// Step 2: Settle — distribute funds to provider and publisher.
	if err := settler.SettleContract(escrowContractIDStr, store.Latest); err != nil {
		return fmt.Errorf("settle contract on-chain: %w", err)
	}

	return nil
}