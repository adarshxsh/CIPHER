package contract

import (
	"testing"
	"time"

	challenge "cipher/availability/availability-contracts/challenge"
	availabilitytypes "cipher/availability/availability-contracts/types"
)

type challengeFlowChunkResolver struct{}

func (challengeFlowChunkResolver) ChunkCount(string, string) (int, error) { return 2, nil }

func TestInitiateAvailabilityChallengeContinuesThenResetsEpoch(t *testing.T) {
	if err := ConfigureChunkCountResolver(challengeFlowChunkResolver{}); err != nil {
		t.Fatalf("ConfigureChunkCountResolver returned error: %v", err)
	}
	contractID, err := CreateAvailabilityContract("publisher-epoch-flow", "provider-epoch-flow", "file-epoch-flow", 100, time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract returned error: %v", err)
	}
	if _, err := FundAvailabilityContract(contractID, 100); err != nil {
		t.Fatalf("FundAvailabilityContract returned error: %v", err)
	}
	_, firstEpochID, err := InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("first InitiateAvailabilityChallenge returned error: %v", err)
	}

	// The round trigger is probabilistic. Force the normal continuation branch
	// so this test deterministically covers the second challenge in one epoch.
	contractStore.Lock()
	tracked := contractStore.activeEpochs[contractID]
	tracked.endAfterCurrent = false
	contractStore.activeEpochs[contractID] = tracked
	contractStore.Unlock()

	_, secondEpochID, err := InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("second InitiateAvailabilityChallenge returned error: %v", err)
	}
	if secondEpochID != firstEpochID {
		t.Fatalf("second challenge epoch = %q, want existing epoch %q", secondEpochID, firstEpochID)
	}
	state, err := challenge.GetEpochState(firstEpochID)
	if err != nil || state.ChallengeCounter != 2 || state.UncoveredChunkCount != 0 {
		t.Fatalf("continued epoch state = %+v, %v; want counter 2 and no uncovered chunks", state, err)
	}
	_, nextEpochID, err := InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("third InitiateAvailabilityChallenge returned error: %v", err)
	}
	if nextEpochID == firstEpochID {
		t.Fatal("exhausted epoch was reused instead of creating a new epoch")
	}
	previousState, err := challenge.GetEpochState(firstEpochID)
	if err != nil || previousState.EpochStatus != availabilitytypes.EpochEnded {
		t.Fatalf("previous epoch state = %+v, %v; want ENDED", previousState, err)
	}
	newState, err := challenge.GetEpochState(nextEpochID)
	if err != nil || newState.ChallengeCounter != 1 || newState.UncoveredChunkCount != 1 {
		t.Fatalf("new epoch state = %+v, %v; want first challenge with one chunk remaining", newState, err)
	}
}
