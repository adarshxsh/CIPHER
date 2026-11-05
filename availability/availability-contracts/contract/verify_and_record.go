// Package contract manages availability-contract records and their public API.
//
// This file (verify_and_record.go) connects the off-chain cryptographic proof verification
// engine (package verification) with the availability contract state machine.
// It allows callers to verify a provider's challenge response and record the result
// in a single atomic operation without modifying existing contract functions.
package contract

import (
	"crypto/ed25519"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	verification "cipher/availability/availability-contracts/verification"
)

// VerifyAndRecordChallengeResponse verifies a provider's chunk possession proof
// against the file Merkle root and the challenge nonce, and immediately records
// the resulting PASS or FAIL into the contract's outcome history.
//
// Verification checks performed:
// 1. Merkle proof: Verifies the provider possesses the chunk matching the file's Merkle root.
// 2. Nonce proof-of-possession: Verifies the chunk was possessed freshly during this challenge round (anti-replay).
// 3. Provider signature: Verifies the response was signed by the authenticated provider's Ed25519 key.
// 4. Freshness/Deadline: Verifies the response was returned before maxTimeout.
//
// Parameters:
// - contractID: The active availability contract identifier.
// - challenge: The challenge issued by InitiateAvailabilityChallenge.
// - response: The provider's cryptographic response.
// - fileMerkleRoot: The authoritative 32-byte Merkle root of the file.
// - providerPubKey: Optional Ed25519 public key of the provider (omitted if nil/empty).
// - maxTimeout: Maximum allowable duration from challenge creation to response.
//
// Returns:
// - The contract's updated lifecycle state.
// - The recorded ChallengeResult (with Succeeded=true for PASS, Succeeded=false for FAIL).
// - Any error encountered during validation or state transition.
func VerifyAndRecordChallengeResponse(
	contractID availabilitytypes.ContractID,
	challenge availabilitytypes.Challenge,
	response verification.ChallengeResponse,
	fileMerkleRoot []byte,
	providerPubKey ed25519.PublicKey,
	maxTimeout time.Duration,
) (availabilitytypes.ContractState, availabilitytypes.ChallengeResult, error) {
	// BuildChallengeResult executes full cryptographic validation:
	// - Returns Succeeded=true if Merkle proof + Nonce + Signature + Deadline pass.
	// - Returns Succeeded=false if any check fails.
	challengeResult := verification.BuildChallengeResult(
		challenge,
		response,
		fileMerkleRoot,
		providerPubKey,
		maxTimeout,
	)

	// RecordAvailabilityResult updates contract state and appends the outcome
	// into contract.Results and contract.AvailabilityResults (for payment consumption).
	nextState, err := RecordAvailabilityResult(contractID, challenge.ChallengeID, challengeResult)
	if err != nil {
		return "", challengeResult, err
	}

	return nextState, challengeResult, nil
}
