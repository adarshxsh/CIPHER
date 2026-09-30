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
	availabilitytypes "cipher/availability/availability-contracts/types"
)

var contractStore = struct {
	sync.RWMutex
	contracts  map[availabilitytypes.ContractID]*availabilitytypes.AvailabilityContract
	challenges map[availabilitytypes.ChallengeID]availabilitytypes.ContractID
}{
	contracts:  make(map[availabilitytypes.ContractID]*availabilitytypes.AvailabilityContract),
	challenges: make(map[availabilitytypes.ChallengeID]availabilitytypes.ContractID),
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
	contract.FundedAmount = paymentAmount
	contract.State = availabilitytypes.Agreed
	return contract.State, nil
}

// InitiateAvailabilityChallenge resolves the file chunk count and delegates
// complete epoch/challenge creation to the challenge module.
func InitiateAvailabilityChallenge(contractID availabilitytypes.ContractID) (availabilitytypes.ChallengeID, availabilitytypes.EpochID, error) {
	contractStore.Lock()
	defer contractStore.Unlock()
	contract, ok := contractStore.contracts[contractID]
	if !ok {
		return "", "", fmt.Errorf("unknown contract: %s", contractID)
	}
	if contract.State != availabilitytypes.Agreed || contract.FundedAmount != contract.PaymentAmount {
		return "", "", errors.New("contract must be agreed and fully funded before a challenge starts")
	}
	if !time.Now().UTC().Before(contract.EndsAt) {
		contract.State = availabilitytypes.Expired
		return "", "", errors.New("contract has expired")
	}
	generated, err := challenge.CreateChallenge(string(contractID), contract.ProviderID, contract.FileID, contract.TotalChunks, 0)
	if err != nil {
		return "", "", fmt.Errorf("create availability challenge: %w", err)
	}
	challengeID := generated.ChallengeID
	contract.Challenges = append(contract.Challenges, challengeID)
	contractStore.challenges[challengeID] = contractID
	contract.State = availabilitytypes.Active
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
	result.ChallengeID = challengeID
	result.RecordedAt = time.Now().UTC()
	contract.Results = append(contract.Results, result)
	return contract.State, nil
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
	contract.Settlement.SuccessfulProofs = 0
	contract.Settlement.FailedProofs = 0
	for _, result := range contract.Results {
		if result.Succeeded {
			contract.Settlement.SuccessfulProofs++
		} else {
			contract.Settlement.FailedProofs++
		}
	}
	contract.Settlement.ReleasedAmount = contract.FundedAmount * int64(contract.Settlement.SuccessfulProofs) / int64(len(contract.Results))
	contract.Settlement.WithheldAmount = contract.FundedAmount - contract.Settlement.ReleasedAmount
	if contract.Settlement.WithheldAmount == 0 {
		contract.Settlement.Status = availabilitytypes.SettlementCompleted
	} else {
		contract.Settlement.Status = availabilitytypes.SettlementWithheld
	}
	if !time.Now().UTC().Before(contract.EndsAt) {
		contract.State = availabilitytypes.Expired
	} else {
		contract.State = availabilitytypes.Completed
	}
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
	contract.State = availabilitytypes.Terminated
	contract.Settlement.Status = availabilitytypes.SettlementTerminated
	contract.Settlement.FinalizedAt = time.Now().UTC()
	return contract.State, nil
}
