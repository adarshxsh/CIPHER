package contract

import (
	"fmt"

	availabilitytypes "cipher/availability/availability-contracts/types"
)

// CanTransition reports whether the finalized contract lifecycle allows a
// direct transition from one state to another.
func CanTransition(from, to availabilitytypes.ContractState) bool {
	if !isValidState(from) || !isValidState(to) || isTerminalState(from) {
		return false
	}
	switch from {
	case availabilitytypes.Proposed:
		return to == availabilitytypes.Agreed || to == availabilitytypes.Terminated || to == availabilitytypes.Disputed
	case availabilitytypes.Agreed:
		return to == availabilitytypes.Transferring || to == availabilitytypes.Terminated || to == availabilitytypes.Disputed
	case availabilitytypes.Transferring:
		return to == availabilitytypes.Ready || to == availabilitytypes.Terminated || to == availabilitytypes.Disputed
	case availabilitytypes.Ready:
		return to == availabilitytypes.Active || to == availabilitytypes.Terminated || to == availabilitytypes.Disputed
	case availabilitytypes.Active:
		return to == availabilitytypes.Completed || to == availabilitytypes.Expired || to == availabilitytypes.Terminated || to == availabilitytypes.Disputed
	}
	return false
}

// Transition validates and returns the next lifecycle state.
func Transition(from, to availabilitytypes.ContractState) (availabilitytypes.ContractState, error) {
	if !CanTransition(from, to) {
		return "", fmt.Errorf("invalid contract state transition: %s -> %s", from, to)
	}
	return to, nil
}
