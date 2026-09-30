// Package coordinator provides the temporary publisher-run workflow layer.
package coordinator

import (
	"errors"
	"sync"

	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

// PublisherCoordinator maps Availability identities to their payment/escrow
// identities. It owns no funds and delegates every release to an adapter.
type PublisherCoordinator struct {
	publisherID string
	signer      payment.StateSigner
	submitter   payment.EscrowSubmitter

	mu        sync.Mutex
	providers map[string]string
	contracts map[string]string
	stores    map[string]payment.PaymentStateStore
}

func NewPublisherCoordinator(publisherID string, signer payment.StateSigner, submitter payment.EscrowSubmitter) (*PublisherCoordinator, error) {
	if publisherID == "" || signer == nil || submitter == nil {
		return nil, errors.New("publisher ID, payment signer, and escrow submitter are required")
	}
	return &PublisherCoordinator{
		publisherID: publisherID, signer: signer, submitter: submitter,
		providers: make(map[string]string), contracts: make(map[string]string), stores: make(map[string]payment.PaymentStateStore),
	}, nil
}

// RegisterProvider binds an Availability peer ID to the provider identity
// expected by the payment/escrow path.
func (c *PublisherCoordinator) RegisterProvider(availabilityProviderID, paymentProviderID string) error {
	if availabilityProviderID == "" || paymentProviderID == "" {
		return errors.New("availability and payment provider IDs are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.providers[availabilityProviderID] = paymentProviderID
	return nil
}

// RegisterContract binds the Availability contract identifier to the escrow
// identifier returned by the eventual chain adapter.
func (c *PublisherCoordinator) RegisterContract(availabilityContractID, escrowContractID string) error {
	if availabilityContractID == "" || escrowContractID == "" {
		return errors.New("availability and escrow contract IDs are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.contracts[availabilityContractID] = escrowContractID
	return nil
}

// ProcessAvailabilityResult creates, signs, stores, and submits the next
// cumulative payment state for a successful Availability period.
func (c *PublisherCoordinator) ProcessAvailabilityResult(result interfaces.AvailabilityResult, cumulativePayment, sequence uint64) (payment.PaymentState, error) {
	if result.Result != interfaces.AvailabilityPass {
		return payment.PaymentState{}, errors.New("failed availability result cannot create a payment state")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	escrowContractID, ok := c.contracts[string(result.ContractID)]
	if !ok {
		return payment.PaymentState{}, errors.New("no escrow contract is registered for availability contract")
	}
	providerID, ok := c.providers[result.ProviderID]
	if !ok {
		return payment.PaymentState{}, errors.New("no payment provider identity is registered")
	}
	result.ProviderID = providerID
	state, err := payment.CreatePaymentState(result, c.publisherID, cumulativePayment, sequence)
	if err != nil {
		return payment.PaymentState{}, err
	}
	state.ContractID = escrowContractID
	signed, err := c.signer.Sign(state)
	if err != nil {
		return payment.PaymentState{}, err
	}
	store := c.stores[escrowContractID]
	if err := store.StorePaymentState(signed); err != nil {
		return payment.PaymentState{}, err
	}
	if err := payment.SubmitPaymentState(escrowContractID, signed, c.submitter); err != nil {
		return payment.PaymentState{}, err
	}
	c.stores[escrowContractID] = store
	return signed, nil
}
