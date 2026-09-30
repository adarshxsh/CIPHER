// Package coordinator provides the temporary publisher-run workflow layer.
package coordinator

import (
	"errors"
	"sync"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	bindings "cipher/availability/escrow-payment/bindings"
	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

// PublisherCoordinator maps Availability identities to their payment/escrow
// identities. It owns no funds and delegates every release to an adapter.
type PublisherCoordinator struct {
	publisherID     string
	signer          payment.StateSigner
	submitter       payment.EscrowSubmitter
	paymentStateTTL time.Duration

	mu       sync.Mutex
	bindings *bindings.Registry
	stores   map[string]payment.PaymentStateStore
}

func NewPublisherCoordinator(publisherID string, signer payment.StateSigner, submitter payment.EscrowSubmitter) (*PublisherCoordinator, error) {
	if publisherID == "" || signer == nil || submitter == nil {
		return nil, errors.New("publisher ID, payment signer, and escrow submitter are required")
	}
	return &PublisherCoordinator{
		publisherID: publisherID, signer: signer, submitter: submitter, paymentStateTTL: time.Hour,
		bindings: bindings.NewRegistry(), stores: make(map[string]payment.PaymentStateStore),
	}, nil
}

// SetPaymentStateTTL sets the authorization lifetime applied to future payment
// states. The value should be consistent with the escrow contract's rules.
func (c *PublisherCoordinator) SetPaymentStateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("payment-state TTL must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paymentStateTTL = ttl
	return nil
}

// RegisterProvider binds an Availability peer ID to the provider identity
// expected by the payment/escrow path.
func (c *PublisherCoordinator) RegisterProvider(availabilityProviderID string, ethereumAddress bindings.EthereumAddress) error {
	return c.bindings.BindProvider(availabilityProviderID, ethereumAddress)
}

// RegisterContract binds the Availability contract identifier to the escrow
// identifier returned by the eventual chain adapter.
func (c *PublisherCoordinator) RegisterContract(availabilityContractID availabilitytypes.ContractID, escrowContractID bindings.EscrowContractID) error {
	return c.bindings.BindContract(availabilityContractID, escrowContractID)
}

// ProcessAvailabilityResult creates, signs, stores, and submits the next
// cumulative payment state for a successful Availability period.
func (c *PublisherCoordinator) ProcessAvailabilityResult(result interfaces.AvailabilityResult, cumulativePayment, sequence uint64) (payment.PaymentState, error) {
	if result.Result != interfaces.AvailabilityPass {
		return payment.PaymentState{}, errors.New("failed availability result cannot create a payment state")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	escrowContractID, ok := c.bindings.EscrowID(result.ContractID)
	if !ok {
		return payment.PaymentState{}, errors.New("no escrow contract is registered for availability contract")
	}
	providerAddress, ok := c.bindings.ProviderAddress(result.ProviderID)
	if !ok {
		return payment.PaymentState{}, errors.New("no payment provider identity is registered")
	}
	result.ProviderID = providerAddress.String()
	state, err := payment.CreatePaymentState(result, c.publisherID, cumulativePayment, sequence)
	if err != nil {
		return payment.PaymentState{}, err
	}
	state.ContractID = escrowContractID.String()
	state.ValidUntil = uint64(time.Now().Add(c.paymentStateTTL).Unix())
	signed, err := c.signer.Sign(state)
	if err != nil {
		return payment.PaymentState{}, err
	}
	store := c.stores[state.ContractID]
	if err := store.StorePaymentState(signed); err != nil {
		return payment.PaymentState{}, err
	}
	if err := payment.SubmitPaymentState(state.ContractID, signed, c.submitter); err != nil {
		return payment.PaymentState{}, err
	}
	c.stores[state.ContractID] = store
	return signed, nil
}
