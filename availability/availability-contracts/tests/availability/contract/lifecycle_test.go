package contract_test

import (
	"testing"

	contract "cipher/availability/availability-contracts/contract"
	availabilitytypes "cipher/availability/availability-contracts/types"
)

func TestContractLifecycle(t *testing.T) {
	state := availabilitytypes.Proposed
	for _, next := range []availabilitytypes.ContractState{
		availabilitytypes.Agreed,
		availabilitytypes.Transferring,
		availabilitytypes.Ready,
		availabilitytypes.Active,
		availabilitytypes.Completed,
	} {
		var err error
		state, err = contract.Transition(state, next)
		if err != nil {
			t.Fatalf("Transition to %s returned error: %v", next, err)
		}
	}
	if !state.IsTerminal() {
		t.Fatal("completed state should be terminal")
	}
}

func TestActiveTerminalTransitions(t *testing.T) {
	for _, next := range []availabilitytypes.ContractState{
		availabilitytypes.Expired,
		availabilitytypes.Terminated,
		availabilitytypes.Disputed,
	} {
		if !contract.CanTransition(availabilitytypes.Active, next) {
			t.Fatalf("ACTIVE -> %s should be allowed", next)
		}
	}
}

func TestInvalidContractTransitions(t *testing.T) {
	if contract.CanTransition(availabilitytypes.Proposed, availabilitytypes.Active) {
		t.Fatal("PROPOSED -> ACTIVE should not be allowed")
	}
	if _, err := contract.Transition(availabilitytypes.Completed, availabilitytypes.Active); err == nil {
		t.Fatal("terminal state transition succeeded")
	}
}
