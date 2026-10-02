// Package contract manages availability-contract records and their public API.
package contract

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	challenge "cipher/availability/availability-contracts/challenge"
	settlement "cipher/availability/availability-contracts/settlement"
	availabilitytypes "cipher/availability/availability-contracts/types"
)

var contractStore = struct {
	sync.RWMutex
	contracts    map[availabilitytypes.ContractID]*availabilitytypes.AvailabilityContract
	challenges   map[availabilitytypes.ChallengeID]availabilitytypes.ContractID
	activeEpochs map[availabilitytypes.ContractID]activeEpoch
}{
	contracts:    make(map[availabilitytypes.ContractID]*availabilitytypes.AvailabilityContract),
	challenges:   make(map[availabilitytypes.ChallengeID]availabilitytypes.ContractID),
	activeEpochs: make(map[availabilitytypes.ContractID]activeEpoch),
}

// activeEpoch is private contract bookkeeping. The challenge package remains
// the source of truth for the counter and uncovered chunk collection.
type activeEpoch struct {
	id              availabilitytypes.EpochID
	endAfterCurrent bool
}

// ChunkCountResolver connects contract initiation to whichever storage/content
// component owns file metadata on the merged branch.
type ChunkCountResolver interface {
	ChunkCount(providerID, fileID string) (int, error)
}

var chunkCountResolver = struct {
	sync.RWMutex
	resolver ChunkCountResolver
}{}

// ConfigureChunkCountResolver supplies the source of file chunk metadata.
// It is the sole integration point required before challenges are initiated.
func ConfigureChunkCountResolver(resolver ChunkCountResolver) error {
	if resolver == nil {
		return errors.New("chunk count resolver is required")
	}
	chunkCountResolver.Lock()
	defer chunkCountResolver.Unlock()
	chunkCountResolver.resolver = resolver
	return nil
}

// CreateAvailabilityContract records a proposed availability commitment.
func CreateAvailabilityContract(publisherID, providerID, fileID string, paymentAmount int64, duration time.Duration) (availabilitytypes.ContractID, error) {
	if publisherID == "" || providerID == "" || fileID == "" {
		return "", errors.New("publisher ID, provider ID, and file ID are required")
	}
	if paymentAmount <= 0 || duration <= 0 {
		return "", errors.New("payment amount and duration must be positive")
	}
	chunkCountResolver.RLock()
	resolver := chunkCountResolver.resolver
	chunkCountResolver.RUnlock()
	if resolver == nil {
		return "", errors.New("no chunk count resolver is configured")
	}
	totalChunks, err := resolver.ChunkCount(providerID, fileID)
	if err != nil {
		return "", fmt.Errorf("resolve file chunk count: %w", err)
	}
	if totalChunks <= 0 {
		return "", errors.New("resolved chunk count must be positive")
	}
	randomID := make([]byte, 16)
	if _, err = rand.Read(randomID); err != nil {
		return "", fmt.Errorf("generate contract ID: %w", err)
	}
	contractID := availabilitytypes.ContractID("contract-" + hex.EncodeToString(randomID))
	now := time.Now().UTC()
	contractStore.Lock()
	defer contractStore.Unlock()
	contractStore.contracts[contractID] = &availabilitytypes.AvailabilityContract{
		ID: contractID, PublisherID: publisherID, ProviderID: providerID, FileID: fileID, TotalChunks: totalChunks,
		PaymentAmount: paymentAmount, Duration: duration, CreatedAt: now, EndsAt: now.Add(duration),
		State:      availabilitytypes.Proposed,
		Settlement: availabilitytypes.SettlementState{Status: availabilitytypes.SettlementPending},
	}
	return contractID, nil
}

// FundAvailabilityContract locks the agreed payment exactly once.
func FundAvailabilityContract(contractID availabilitytypes.ContractID, paymentAmount int64) (availabilitytypes.ContractState, error) {
	contractStore.Lock()
	defer contractStore.Unlock()
	contract, ok := contractStore.contracts[contractID]
	if !ok {
		return "", fmt.Errorf("unknown contract: %s", contractID)
	}
	if contract.State != availabilitytypes.Proposed {
		return "", fmt.Errorf("contract %s cannot be funded from state %s", contractID, contract.State)
	}
	if paymentAmount != contract.PaymentAmount {
		return "", errors.New("funding amount must equal the agreed payment amount")
	}
	nextState, err := Transition(contract.State, availabilitytypes.Agreed)
	if err != nil {
		return "", err
	}
	contract.FundedAmount = paymentAmount
	contract.State = nextState
	return contract.State, nil
}

// InitiateAvailabilityChallenge creates the first challenge in an epoch, then
// continues that epoch until it is exhausted or a round trigger ends it.
func InitiateAvailabilityChallenge(contractID availabilitytypes.ContractID) (availabilitytypes.ChallengeID, availabilitytypes.EpochID, error) {
	contractStore.Lock()
	defer contractStore.Unlock()
	contract, ok := contractStore.contracts[contractID]
	if !ok {
		return "", "", fmt.Errorf("unknown contract: %s", contractID)
	}
	if (contract.State != availabilitytypes.Agreed && contract.State != availabilitytypes.Active) || contract.FundedAmount != contract.PaymentAmount {
		return "", "", errors.New("contract must be agreed or active and fully funded before a challenge starts")
	}
	if !time.Now().UTC().Before(contract.EndsAt) {
		contract.State = availabilitytypes.Expired
		return "", "", errors.New("contract has expired")
	}
	trackedEpoch, hasActiveEpoch := contractStore.activeEpochs[contractID]
	generated := availabilitytypes.Challenge{}
	var err error
	if hasActiveEpoch {
		epochState, stateErr := challenge.GetEpochState(trackedEpoch.id)
		if stateErr != nil {
			return "", "", fmt.Errorf("get active epoch state: %w", stateErr)
		}
		if epochState.EpochStatus == availabilitytypes.EpochActive && epochState.UncoveredChunkCount > 0 && !trackedEpoch.endAfterCurrent {
			generated, err = challenge.GenerateNextChallenge(string(contractID), contract.ProviderID, contract.FileID, trackedEpoch.id, epochState.ChallengeCounter, epochState.RoundNumber)
		} else {
			generated, err = challenge.CreateChallenge(string(contractID), contract.ProviderID, contract.FileID, contract.TotalChunks, epochState.RoundNumber+1)
		}
	} else {
		generated, err = challenge.CreateChallenge(string(contractID), contract.ProviderID, contract.FileID, contract.TotalChunks, 0)
	}
	if err != nil {
		return "", "", fmt.Errorf("generate availability challenge: %w", err)
	}
	challengeID := generated.ChallengeID
	if contract.State == availabilitytypes.Agreed {
		for _, nextState := range []availabilitytypes.ContractState{availabilitytypes.Transferring, availabilitytypes.Ready, availabilitytypes.Active} {
			contract.State, err = Transition(contract.State, nextState)
			if err != nil {
				return "", "", err
			}
		}
	}
	contract.Challenges = append(contract.Challenges, challengeID)
	contractStore.challenges[challengeID] = contractID
	contractStore.activeEpochs[contractID] = activeEpoch{id: generated.EpochID, endAfterCurrent: generated.TriggerStatus}
	return challengeID, generated.EpochID, nil
}

// RecordAvailabilityResult stores a provider result; validation of the proof
// itself remains outside the contract module.
func RecordAvailabilityResult(contractID availabilitytypes.ContractID, challengeID availabilitytypes.ChallengeID, result availabilitytypes.ChallengeResult) (availabilitytypes.ContractState, error) {
	contractStore.Lock()
	defer contractStore.Unlock()
	contract, ok := contractStore.contracts[contractID]
	if !ok {
		return "", fmt.Errorf("unknown contract: %s", contractID)
	}
	if contract.State != availabilitytypes.Active {
		return "", fmt.Errorf("contract %s is not active", contractID)
	}
	if owner, exists := contractStore.challenges[challengeID]; !exists || owner != contractID {
		return "", errors.New("challenge does not belong to contract")
	}
	if result.ChallengeID != "" && result.ChallengeID != challengeID {
		return "", errors.New("result challenge ID does not match")
	}
	for _, recorded := range contract.Results {
		if recorded.ChallengeID == challengeID {
			return "", errors.New("challenge result has already been recorded")
		}
	}
	result.ChallengeID = challengeID
	result.RecordedAt = time.Now().UTC()
	contract.Results = append(contract.Results, result)
	availabilityStatus := availabilitytypes.AvailabilityFail
	if result.Succeeded {
		availabilityStatus = availabilitytypes.AvailabilityPass
	}
	contract.AvailabilityResults = append(contract.AvailabilityResults, availabilitytypes.AvailabilityResult{
		ContractID: contractID, ProviderID: contract.ProviderID,
		Period: uint64(len(contract.AvailabilityResults) + 1), ChallengeID: challengeID,
		Result: availabilityStatus, Timestamp: result.RecordedAt,
	})
	return contract.State, nil
}

// GetLatestAvailabilityResult returns the standardized output intended for the
// separate payment module after RecordAvailabilityResult succeeds.
func GetLatestAvailabilityResult(contractID availabilitytypes.ContractID) (availabilitytypes.AvailabilityResult, error) {
	contractStore.RLock()
	defer contractStore.RUnlock()
	contract, ok := contractStore.contracts[contractID]
	if !ok {
		return availabilitytypes.AvailabilityResult{}, fmt.Errorf("unknown contract: %s", contractID)
	}
	if len(contract.AvailabilityResults) == 0 {
		return availabilitytypes.AvailabilityResult{}, errors.New("contract has no recorded availability result")
	}
	return contract.AvailabilityResults[len(contract.AvailabilityResults)-1], nil
}

// SettleAvailabilityContract proportionally releases the funded amount based
// on recorded results. This avoids treating one failed challenge as a full
// automatic penalty while keeping settlement policy replaceable later.
func SettleAvailabilityContract(contractID availabilitytypes.ContractID) (availabilitytypes.SettlementStatus, error) {
	contractStore.Lock()
	defer contractStore.Unlock()
	contract, ok := contractStore.contracts[contractID]
	if !ok {
		return "", fmt.Errorf("unknown contract: %s", contractID)
	}
	if contract.State != availabilitytypes.Active && contract.State != availabilitytypes.Expired {
		return "", fmt.Errorf("contract %s cannot be settled from state %s", contractID, contract.State)
	}
	if len(contract.Results) == 0 {
		return "", errors.New("cannot settle without recorded challenge results")
	}
	settlementState, err := settlement.CalculateSettlement(contract.FundedAmount, contract.Results)
	if err != nil {
		return "", err
	}
	contract.Settlement = settlementState
	var nextState availabilitytypes.ContractState
	if !time.Now().UTC().Before(contract.EndsAt) {
		nextState, err = Transition(contract.State, availabilitytypes.Expired)
	} else {
		nextState, err = Transition(contract.State, availabilitytypes.Completed)
	}
	if err != nil {
		return "", err
	}
	contract.State = nextState
	contract.Settlement.FinalizedAt = time.Now().UTC()
	return contract.Settlement.Status, nil
}

// TerminateAvailabilityContract ends a non-terminal contract.
func TerminateAvailabilityContract(contractID availabilitytypes.ContractID) (availabilitytypes.ContractState, error) {
	contractStore.Lock()
	defer contractStore.Unlock()
	contract, ok := contractStore.contracts[contractID]
	if !ok {
		return "", fmt.Errorf("unknown contract: %s", contractID)
	}
	if contract.State.IsTerminal() {
		return "", fmt.Errorf("contract %s is already terminal", contractID)
	}
	nextState, err := Transition(contract.State, availabilitytypes.Terminated)
	if err != nil {
		return "", err
	}
	contract.State = nextState
	contract.Settlement.Status = availabilitytypes.SettlementTerminated
	contract.Settlement.FinalizedAt = time.Now().UTC()
	return contract.State, nil
}
