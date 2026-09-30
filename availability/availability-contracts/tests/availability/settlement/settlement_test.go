package settlement_test

import (
	"testing"

	settlement "cipher/availability/availability-contracts/settlement"
	availabilitytypes "cipher/availability/availability-contracts/types"
)

func TestCalculateSettlement(t *testing.T) {
	tests := []struct {
		name     string
		results  []availabilitytypes.ChallengeResult
		released int64
		withheld int64
		status   availabilitytypes.SettlementStatus
	}{
		{name: "all successful", results: []availabilitytypes.ChallengeResult{{Succeeded: true}, {Succeeded: true}}, released: 100, withheld: 0, status: availabilitytypes.SettlementCompleted},
		{name: "single failure is proportional", results: []availabilitytypes.ChallengeResult{{Succeeded: true}, {Succeeded: true}, {Succeeded: false}}, released: 66, withheld: 34, status: availabilitytypes.SettlementWithheld},
		{name: "all failed", results: []availabilitytypes.ChallengeResult{{Succeeded: false}, {Succeeded: false}}, released: 0, withheld: 100, status: availabilitytypes.SettlementWithheld},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, err := settlement.CalculateSettlement(100, test.results)
			if err != nil {
				t.Fatalf("CalculateSettlement returned error: %v", err)
			}
			if state.ReleasedAmount != test.released || state.WithheldAmount != test.withheld || state.Status != test.status {
				t.Fatalf("settlement = %+v, want released=%d withheld=%d status=%s", state, test.released, test.withheld, test.status)
			}
			if state.SuccessfulProofs+state.FailedProofs != len(test.results) {
				t.Fatalf("proof counts = %d successes + %d failures, want %d results", state.SuccessfulProofs, state.FailedProofs, len(test.results))
			}
		})
	}
}

func TestCalculateSettlementUsesIntegerRounding(t *testing.T) {
	state, err := settlement.CalculateSettlement(101, []availabilitytypes.ChallengeResult{{Succeeded: true}, {Succeeded: true}, {Succeeded: false}})
	if err != nil {
		t.Fatalf("CalculateSettlement returned error: %v", err)
	}
	if state.ReleasedAmount != 67 || state.WithheldAmount != 34 {
		t.Fatalf("rounded settlement = %+v, want released=67 withheld=34", state)
	}
}

func TestCalculateSettlementRejectsInvalidInput(t *testing.T) {
	if _, err := settlement.CalculateSettlement(0, []availabilitytypes.ChallengeResult{{Succeeded: true}}); err == nil {
		t.Fatal("CalculateSettlement accepted a zero funded amount")
	}
	if _, err := settlement.CalculateSettlement(100, nil); err == nil {
		t.Fatal("CalculateSettlement accepted no results")
	}
}
