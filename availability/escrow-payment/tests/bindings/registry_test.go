package bindings_test

import (
	"testing"

	availabilitytypes "cipher/availability/availability-contracts/types"
	bindings "cipher/availability/escrow-payment/bindings"
)

func TestRegistry_RebindingProtection(t *testing.T) {
	registry := bindings.NewRegistry()

	addr1, err := bindings.ParseEthereumAddress("0x1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("ParseEthereumAddress returned error: %v", err)
	}
	addr2, err := bindings.ParseEthereumAddress("0x2222222222222222222222222222222222222222")
	if err != nil {
		t.Fatalf("ParseEthereumAddress returned error: %v", err)
	}

	// 1. Initial bind of provider succeeds.
	if err := registry.BindProvider("peer-1", addr1); err != nil {
		t.Fatalf("first BindProvider failed: %v", err)
	}

	// 2. Idempotent re-bind with identical address succeeds.
	if err := registry.BindProvider("peer-1", addr1); err != nil {
		t.Fatalf("idempotent BindProvider failed: %v", err)
	}

	// 3. Bug 9 fix: Re-bind with different address is rejected.
	if err := registry.BindProvider("peer-1", addr2); err == nil {
		t.Fatal("expected error on re-binding peer-1 to different address, got nil")
	}

	// Check provider address remains addr1.
	gotAddr, ok := registry.ProviderAddress("peer-1")
	if !ok || gotAddr != addr1 {
		t.Fatalf("expected provider address %v, got %v", addr1, gotAddr)
	}

	escrow1, err := bindings.ParseEscrowContractID("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("ParseEscrowContractID returned error: %v", err)
	}
	escrow2, err := bindings.ParseEscrowContractID("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatalf("ParseEscrowContractID returned error: %v", err)
	}

	availContractID := availabilitytypes.ContractID("avail-contract-1")

	// 4. Initial bind of contract succeeds.
	if err := registry.BindContract(availContractID, escrow1); err != nil {
		t.Fatalf("first BindContract failed: %v", err)
	}

	// 5. Idempotent re-bind of identical contract succeeds.
	if err := registry.BindContract(availContractID, escrow1); err != nil {
		t.Fatalf("idempotent BindContract failed: %v", err)
	}

	// 6. Bug 9 fix: Re-bind of different escrow ID is rejected.
	if err := registry.BindContract(availContractID, escrow2); err == nil {
		t.Fatal("expected error on re-binding contract to different escrow ID, got nil")
	}
}
