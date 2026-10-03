// Package bindings owns the explicit mapping between off-chain CIPHER
// identities and their on-chain escrow identities.
package bindings

import (
	"encoding/hex"
	"errors"
	"sync"

	availabilitytypes "cipher/availability/availability-contracts/types"
)

type EthereumAddress [20]byte
type EscrowContractID [32]byte

func ParseEthereumAddress(value string) (EthereumAddress, error) {
	var address EthereumAddress
	if len(value) == 42 && value[:2] == "0x" {
		value = value[2:]
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(address) {
		return EthereumAddress{}, errors.New("Ethereum address must be exactly 20 bytes of hex")
	}
	copy(address[:], decoded)
	return address, nil
}

func (address EthereumAddress) String() string { return "0x" + hex.EncodeToString(address[:]) }

func ParseEscrowContractID(value string) (EscrowContractID, error) {
	var contractID EscrowContractID
	if len(value) == 66 && value[:2] == "0x" {
		value = value[2:]
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(contractID) {
		return EscrowContractID{}, errors.New("escrow contract ID must be exactly 32 bytes of hex")
	}
	copy(contractID[:], decoded)
	return contractID, nil
}

func (contractID EscrowContractID) String() string { return "0x" + hex.EncodeToString(contractID[:]) }

// Registry is an explicit coordinator-managed binding registry. Contract IDs
// must come from EscrowContract.createContract; they are never derived from a
// CIPHER ID locally.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]EthereumAddress
	contracts map[availabilitytypes.ContractID]EscrowContractID
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]EthereumAddress), contracts: make(map[availabilitytypes.ContractID]EscrowContractID)}
}

// BindProvider binds an Availability peer ID to an Ethereum address.
// Re-binding the same peer ID to the same address is idempotent.
// Re-binding to a different address is rejected to prevent silent identity hijacking.
func (r *Registry) BindProvider(peerID string, address EthereumAddress) error {
	if peerID == "" || address == (EthereumAddress{}) {
		return errors.New("peer ID and non-zero Ethereum address are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.providers[peerID]; ok {
		if existing != address {
			return errors.New("peer ID is already bound to a different Ethereum address")
		}
		return nil // Idempotent re-registration
	}

	r.providers[peerID] = address
	return nil
}

// BindContract binds an Availability contract ID to an on-chain escrow ID.
// Re-binding the same contract ID to the same escrow contract is idempotent.
// Re-binding to a different escrow ID is rejected to prevent silent rebinding.
func (r *Registry) BindContract(contractID availabilitytypes.ContractID, escrowContractID EscrowContractID) error {
	if contractID == "" || escrowContractID == (EscrowContractID{}) {
		return errors.New("availability contract ID and non-zero escrow contract ID are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.contracts[contractID]; ok {
		if existing != escrowContractID {
			return errors.New("contract ID is already bound to a different escrow contract")
		}
		return nil // Idempotent re-registration
	}

	r.contracts[contractID] = escrowContractID
	return nil
}

func (r *Registry) ProviderAddress(peerID string) (EthereumAddress, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	address, ok := r.providers[peerID]
	return address, ok
}

func (r *Registry) EscrowID(contractID availabilitytypes.ContractID) (EscrowContractID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	escrowContractID, ok := r.contracts[contractID]
	return escrowContractID, ok
}
