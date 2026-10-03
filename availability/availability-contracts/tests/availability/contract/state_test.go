package contract_test

import (
	"testing"

	availabilitytypes "cipher/availability/availability-contracts/types"
)

func TestTerminalContractStates(t *testing.T) {
	terminalStates := []availabilitytypes.ContractState{
		availabilitytypes.Completed,
		availabilitytypes.Expired,
		availabilitytypes.Terminated,
		availabilitytypes.Disputed,
	}
	for _, state := range terminalStates {
		if !state.IsTerminal() {
			t.Fatalf("state %s should be terminal", state)
		}
	}

	nonTerminalStates := []availabilitytypes.ContractState{
		availabilitytypes.Proposed,
		availabilitytypes.Agreed,
		availabilitytypes.Transferring,
		availabilitytypes.Ready,
		availabilitytypes.Active,
	}
	for _, state := range nonTerminalStates {
		if state.IsTerminal() {
			t.Fatalf("state %s should not be terminal", state)
		}
	}
}
