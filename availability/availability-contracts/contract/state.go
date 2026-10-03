package contract

import availabilitytypes "cipher/availability/availability-contracts/types"

func isValidState(state availabilitytypes.ContractState) bool {
	switch state {
	case availabilitytypes.Proposed,
		availabilitytypes.Agreed,
		availabilitytypes.Transferring,
		availabilitytypes.Ready,
		availabilitytypes.Active,
		availabilitytypes.Completed,
		availabilitytypes.Expired,
		availabilitytypes.Terminated,
		availabilitytypes.Disputed:
		return true
	default:
		return false
	}
}

func isTerminalState(state availabilitytypes.ContractState) bool {
	return state == availabilitytypes.Completed ||
		state == availabilitytypes.Expired ||
		state == availabilitytypes.Terminated ||
		state == availabilitytypes.Disputed
}
