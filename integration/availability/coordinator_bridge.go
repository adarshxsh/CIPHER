package availability

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"errors"
	"fmt"

	availabilitytypes "cipher/availability/availability-contracts/types"
	bindings "cipher/availability/escrow-payment/bindings"
	coordinator "cipher/availability/escrow-payment/coordinator"
	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// NoopEscrowSubmitter is an in-memory submitter used for off-chain optimistic operations.
type NoopEscrowSubmitter struct{}

func (n NoopEscrowSubmitter) SubmitPaymentState(contractID string, state payment.PaymentState) error {
	return nil
}

// CoordinatorBridge integrates CIPHER Dual Identity (Ed25519 + Secp256k1)
// with the Availability PublisherCoordinator and AvailabilityPaymentDispatcher.
type CoordinatorBridge struct {
	publisherID     string
	publisherEthKey *ecdsa.PrivateKey
	publisherEdKey  ed25519.PrivateKey
	coordinator     *coordinator.PublisherCoordinator
	dispatcher      *coordinator.AvailabilityPaymentDispatcher
	submitter       payment.EscrowSubmitter
}

// CoordinatorBridgeConfig contains configuration parameters for the bridge.
type CoordinatorBridgeConfig struct {
	PublisherPeerID  string
	PublisherEthKey  *ecdsa.PrivateKey
	PublisherEdKey   ed25519.PrivateKey
	EscrowSubmitter  payment.EscrowSubmitter
	FailureThreshold uint32 // K consecutive failures before slash eligibility (default 3)
}

// NewCoordinatorBridge creates a fully wired coordinator bridge.
func NewCoordinatorBridge(cfg CoordinatorBridgeConfig) (*CoordinatorBridge, error) {
	if cfg.PublisherPeerID == "" {
		return nil, errors.New("publisher peer ID is required")
	}
	if len(cfg.PublisherEdKey) != ed25519.PrivateKeySize {
		return nil, errors.New("valid publisher Ed25519 private key is required")
	}

	submitter := cfg.EscrowSubmitter
	if submitter == nil {
		submitter = NoopEscrowSubmitter{}
	}

	// Use Ed25519 signer for vouchers by default
	signer := payment.Ed25519Signer{PrivateKey: cfg.PublisherEdKey}

	pubCoord, err := coordinator.NewPublisherCoordinator(cfg.PublisherPeerID, signer, submitter)
	if err != nil {
		return nil, fmt.Errorf("create publisher coordinator: %w", err)
	}

	threshold := cfg.FailureThreshold
	if threshold == 0 {
		threshold = coordinator.DefaultFailureThreshold
	}

	disp, err := coordinator.NewAvailabilityPaymentDispatcher(
		pubCoord,
		cfg.PublisherEdKey,
		coordinator.WithFailureThreshold(threshold),
	)
	if err != nil {
		return nil, fmt.Errorf("create payment dispatcher: %w", err)
	}

	return &CoordinatorBridge{
		publisherID:     cfg.PublisherPeerID,
		publisherEthKey: cfg.PublisherEthKey,
		publisherEdKey:  cfg.PublisherEdKey,
		coordinator:     pubCoord,
		dispatcher:      disp,
		submitter:       submitter,
	}, nil
}

// RegisterProvider binds a provider's libp2p Peer ID to its on-chain Ethereum payout address.
func (b *CoordinatorBridge) RegisterProvider(peerID string, ethAddr common.Address) error {
	ethBinding, err := bindings.ParseEthereumAddress(ethAddr.Hex())
	if err != nil {
		return fmt.Errorf("parse provider ethereum address: %w", err)
	}
	return b.coordinator.RegisterProvider(peerID, ethBinding)
}

// RegisterContractSchedule binds an Availability contract ID to an Escrow agreement ID
// and registers the pass-through payment schedule.
func (b *CoordinatorBridge) RegisterContractSchedule(
	contractID availabilitytypes.ContractID,
	escrowAgreementID [32]byte,
	paymentPerPass uint64,
	maxPayment uint64,
) error {
	escrowBinding, err := bindings.ParseEscrowContractID(common.BytesToHash(escrowAgreementID[:]).Hex())
	if err != nil {
		return fmt.Errorf("parse escrow contract ID: %w", err)
	}

	return b.dispatcher.RegisterContractSchedule(contractID, escrowBinding, paymentPerPass, maxPayment)
}

// DispatchAvailabilityResult processes a verified AvailabilityResult, producing either
// an off-chain signed PaymentState voucher (on PASS) or a signed FailRecord (on FAIL).
func (b *CoordinatorBridge) DispatchAvailabilityResult(res availabilitytypes.AvailabilityResult) (coordinator.DispatchResult, error) {
	return b.dispatcher.DispatchAvailabilityResult(interfaces.AvailabilityResult(res))
}

// Coordinator returns the underlying PublisherCoordinator.
func (b *CoordinatorBridge) Coordinator() *coordinator.PublisherCoordinator {
	return b.coordinator
}

// Dispatcher returns the underlying AvailabilityPaymentDispatcher.
func (b *CoordinatorBridge) Dispatcher() *coordinator.AvailabilityPaymentDispatcher {
	return b.dispatcher
}

// VerifyPaymentVoucher validates an off-chain PaymentState signature against the publisher's identity.
func VerifyPaymentVoucher(voucher payment.PaymentState, pubEdKey ed25519.PublicKey, pubEthAddr common.Address) bool {
	if len(voucher.PublisherSignature) == 0 {
		return false
	}

	// 1. Try Ed25519 signature verification first
	if len(pubEdKey) == ed25519.PublicKeySize {
		// Verify using payment package verifier
		if payment.VerifyPaymentState(voucher, pubEdKey) {
			return true
		}
	}


	// 2. Try Ethereum Secp256k1 recovery
	if pubEthAddr != (common.Address{}) && len(voucher.PublisherSignature) == 65 {
		sig := voucher.PublisherSignature
		recoveredPub, err := crypto.SigToPub(crypto.Keccak256([]byte(voucher.ContractID)), sig)
		if err == nil {
			recoveredAddr := crypto.PubkeyToAddress(*recoveredPub)
			if recoveredAddr == pubEthAddr {
				return true
			}
		}
	}

	return false
}
