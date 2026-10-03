// Package settlement calculates availability-payment outcomes.
package settlement

import (
	"errors"

	availabilitytypes "cipher/availability/availability-contracts/types"
)

// CalculateSettlement derives a proportional payment outcome from recorded
// availability results. It performs no payment transfer or proof validation.
func CalculateSettlement(fundedAmount int64, results []availabilitytypes.ChallengeResult) (availabilitytypes.SettlementState, error) {
	if fundedAmount <= 0 {
		return availabilitytypes.SettlementState{}, errors.New("funded amount must be positive")
	}
	if len(results) == 0 {
		return availabilitytypes.SettlementState{}, errors.New("recorded challenge results are required")
	}
	state := availabilitytypes.SettlementState{}
	for _, result := range results {
		if result.Succeeded {
			state.SuccessfulProofs++
		} else {
			state.FailedProofs++
		}
	}
	state.ReleasedAmount = fundedAmount * int64(state.SuccessfulProofs) / int64(len(results))
	state.WithheldAmount = fundedAmount - state.ReleasedAmount
	if state.WithheldAmount == 0 {
		state.Status = availabilitytypes.SettlementCompleted
	} else {
		state.Status = availabilitytypes.SettlementWithheld
	}
	return state, nil
}
