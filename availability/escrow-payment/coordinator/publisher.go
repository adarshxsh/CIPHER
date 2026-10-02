// Package coordinator provides the temporary publisher-run workflow layer.
package coordinator

import (
	"errors"
	"sync"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	bindings "cipher/availability/escrow-payment/bindings"
	payment "cipher/availability/escrow-payment/payment"
)

// PublisherCoordinator maps Availability identities to their payment/escrow
// identities. It owns no funds and delegates every release to an adapter.
//paymentStateTTL: 30 * 24 * time.Hour - if you want for 30 days in NewPublisherCoordinator()
//each payment session will gets its own TTL fo one day for now
type PublisherCoordinator struct {
	publisherID     string
	signer          payment.StateSigner
	submitter       payment.EscrowSubmitter
	paymentStateTTL time.Duration

	// settler is the optional on-chain settlement adapter. When set, the
	// coordinator can perform the full two-step settlement flow:
	//   1. submitPaymentState (record the latest voucher on-chain)
	//   2. settleContract     (distribute funds to provider and publisher)
	// If nil, only step 1 is available via SubmitLatestPaymentStateOnChain.
	settler payment.EscrowSettler

	// now is injectable for deterministic tests.
	// If nil, currentTime() uses time.Now().
	now func() time.Time

	mu       sync.Mutex
	bindings *bindings.Registry
	stores   map[string]payment.PaymentStateStore
}

func NewPublisherCoordinator(
	publisherID string,
	signer payment.StateSigner,
	submitter payment.EscrowSubmitter,
) (*PublisherCoordinator, error) {

	if publisherID == "" || signer == nil || submitter == nil {
		return nil, errors.New(
			"publisher ID, payment signer, and escrow submitter are required",
		)
	}

	return &PublisherCoordinator{
		publisherID:     publisherID,
		signer:          signer,
		submitter:       submitter,
		paymentStateTTL: 24 * time.Hour,
		bindings:        bindings.NewRegistry(),
		stores:          make(map[string]payment.PaymentStateStore),
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
func (c *PublisherCoordinator) RegisterProvider(
	availabilityProviderID string,
	ethereumAddress bindings.EthereumAddress,
) error {
	return c.bindings.BindProvider(
		availabilityProviderID,
		ethereumAddress,
	)
}

// RegisterContract binds the Availability contract identifier to the escrow
// identifier returned by the eventual chain adapter.
func (c *PublisherCoordinator) RegisterContract(
	availabilityContractID availabilitytypes.ContractID,
	escrowContractID bindings.EscrowContractID,
) error {

	if err := c.bindings.BindContract(
		availabilityContractID,
		escrowContractID,
	); err != nil {
		return err
	}

	// Initialize the payment-state store for this escrow contract.
	// Do not overwrite an existing store because it may already contain
	// the latest signed payment state.
	c.mu.Lock()
	defer c.mu.Unlock()

	escrowID := escrowContractID.String()

	if _, exists := c.stores[escrowID]; !exists {
		c.stores[escrowID] = payment.PaymentStateStore{}
	}

	return nil
}