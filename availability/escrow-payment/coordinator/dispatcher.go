// Package coordinator provides the temporary publisher-run workflow layer.
//
// This file connects verified Availability results to the off-chain payment
// system using the OPTIMISTIC OFF-CHAIN model.
//
// On-chain interaction happens ONLY at:
//  1. CONTRACT SETTLEMENT: provider submits their latest signed PaymentState.
//  2. DISPUTE / SLASHING: see the K-consecutive-failure rule below.
//
// Happy path:
//
//	Challenge → Provider proves chunk → Publisher signs PaymentState off-chain
//	→ sent to Provider via P2P → (repeat) → Provider settles on-chain.
//
// FAIL path:
//
//	Challenge → Provider fails → Publisher signs FailRecord off-chain
//	→ sent to Provider via P2P → Provider accepts or disputes.
//
// SLASHING RULE:
//
//   - A single FAIL only forfeits that round's payment.
//   - Any PASS resets the consecutive-failure counter.
//   - When the counter reaches K, the result is flagged SlashEligible and
//     carries the last K FailRecords as evidence.
//   - The dispatcher NEVER sends a slash transaction.
//
// CONSISTENCY GUARANTEE:
//
// DispatchAvailabilityResult validates inputs and creates the signed artifact
// before committing schedule state. If artifact creation fails, the schedule
// is unchanged and the result may be retried.
package coordinator

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	bindings "cipher/availability/escrow-payment/bindings"
	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

// DefaultFailureThreshold is the default K for the consecutive-failure rule.
const DefaultFailureThreshold uint32 = 3

// failRecordDomainTag prevents a FailRecord signature from being reused as
// a signature for another message type.
const failRecordDomainTag = "CIPHER_FAIL_RECORD_V1"

// Sentinel errors.
var (
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrScheduleExists     = errors.New("payment schedule already registered for contract")
	ErrScheduleNotFound   = errors.New("no payment schedule registered for contract")
	ErrDuplicateChallenge = errors.New("challenge result already dispatched")
	ErrUnknownStatus      = errors.New("unrecognized availability status")
	ErrSequenceExhausted  = errors.New("sequence counter exhausted")
)

// FailRecord is a publisher-signed off-chain record asserting that a specific
// Availability challenge was not passed by the provider.
type FailRecord struct {
	EscrowContractID   string
	ProviderEthAddress string
	ChallengeID        availabilitytypes.ChallengeID
	Period             uint64
	Sequence           uint64
	IssuedAt           time.Time
	PublisherSignature []byte
}

// DispatchKind identifies the artifact produced by a dispatch.
type DispatchKind string

const (
	DispatchKindPass DispatchKind = "PASS"
	DispatchKindFail DispatchKind = "FAIL"
)

// DispatchResult is the unified output of DispatchAvailabilityResult.
// Exactly one of PaymentState or FailRecord is populated.
type DispatchResult struct {
	Kind                DispatchKind
	PaymentState        payment.PaymentState
	FailRecord          FailRecord
	ConsecutiveFailures uint32
	SlashEligible       bool
	SlashEvidence       []FailRecord
}

// PaymentSchedule tracks the economic configuration and running state for
// an active Availability agreement.
type PaymentSchedule struct {
	PaymentPerPass          uint64
	MaxPayment              uint64
	CurrentCumulativePayment uint64
	Sequence                uint64
	EscrowContractID        bindings.EscrowContractID
	ProcessedChallenges     map[availabilitytypes.ChallengeID]bool
	ConsecutiveFailures     uint32
	FailStreak              []FailRecord
}

// AvailabilityPaymentDispatcher bridges verified Availability results and
// the off-chain payment system.
//
// No blockchain transaction is triggered by DispatchAvailabilityResult.
type AvailabilityPaymentDispatcher struct {
	mu          sync.Mutex
	coordinator *PublisherCoordinator
	schedules   map[availabilitytypes.ContractID]*PaymentSchedule

	// signerKey signs FailRecords using the publisher's Ed25519 key.
	signerKey ed25519.PrivateKey

	// failureThreshold is K.
	failureThreshold uint32

	// now is injectable for deterministic FailRecord timestamps.
	now func() time.Time
}

// DispatcherOption configures an AvailabilityPaymentDispatcher.
type DispatcherOption func(*AvailabilityPaymentDispatcher) error

// WithFailureThreshold sets K, the number of consecutive failures required
// before a result becomes SlashEligible.
func WithFailureThreshold(k uint32) DispatcherOption {
	return func(d *AvailabilityPaymentDispatcher) error {
		if k == 0 {
			return fmt.Errorf(
				"%w: failure threshold must be at least 1",
				ErrInvalidArgument,
			)
		}

		d.failureThreshold = k
		return nil
	}
}

// NewAvailabilityPaymentDispatcher creates a dispatcher wrapping a
// PublisherCoordinator.
func NewAvailabilityPaymentDispatcher(
	coordinator *PublisherCoordinator,
	signerKey ed25519.PrivateKey,
	opts ...DispatcherOption,
) (*AvailabilityPaymentDispatcher, error) {

	if coordinator == nil {
		return nil, fmt.Errorf(
			"%w: publisher coordinator is required",
			ErrInvalidArgument,
		)
	}

	if len(signerKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf(
			"%w: valid publisher Ed25519 private key is required for signing FailRecords",
			ErrInvalidArgument,
		)
	}

	// Defensive copy of the private key.
	keyCopy := make(ed25519.PrivateKey, len(signerKey))
	copy(keyCopy, signerKey)

	d := &AvailabilityPaymentDispatcher{
		coordinator:      coordinator,
		schedules:        make(map[availabilitytypes.ContractID]*PaymentSchedule),
		signerKey:        keyCopy,
		failureThreshold: DefaultFailureThreshold,
		now:              func() time.Time { return time.Now().UTC() },
	}

	for _, opt := range opts {
		if err := opt(d); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// RegisterContractSchedule registers the escrow binding and economic payout
// schedule before challenges begin.
func (d *AvailabilityPaymentDispatcher) RegisterContractSchedule(
	availabilityContractID availabilitytypes.ContractID,
	escrowContractID bindings.EscrowContractID,
	paymentPerPass uint64,
	maxPayment uint64,
) error {

	if availabilityContractID == "" ||
		escrowContractID == (bindings.EscrowContractID{}) {
		return fmt.Errorf(
			"%w: valid availability contract ID and escrow contract ID are required",
			ErrInvalidArgument,
		)
	}

	if paymentPerPass == 0 || maxPayment == 0 {
		return fmt.Errorf(
			"%w: payment per pass and max payment must be positive",
			ErrInvalidArgument,
		)
	}

	if paymentPerPass > maxPayment {
		return fmt.Errorf(
			"%w: payment per pass cannot exceed max payment",
			ErrInvalidArgument,
		)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check before touching coordinator state.
	if _, exists := d.schedules[availabilityContractID]; exists {
		return fmt.Errorf(
			"%w: %s",
			ErrScheduleExists,
			availabilityContractID,
		)
	}

	if err := d.coordinator.RegisterContract(
		availabilityContractID,
		escrowContractID,
	); err != nil {
		return fmt.Errorf(
			"register contract in coordinator: %w",
			err,
		)
	}

	d.schedules[availabilityContractID] = &PaymentSchedule{
		PaymentPerPass:      paymentPerPass,
		MaxPayment:          maxPayment,
		EscrowContractID:    escrowContractID,
		ProcessedChallenges: make(map[availabilitytypes.ChallengeID]bool),
	}

	return nil
}

// validateResult performs stateless input validation.
func validateResult(result interfaces.AvailabilityResult) error {
	if result.ContractID == "" {
		return fmt.Errorf(
			"%w: result has empty contract ID",
			ErrInvalidArgument,
		)
	}

	if result.ChallengeID == "" {
		return fmt.Errorf(
			"%w: result has empty challenge ID",
			ErrInvalidArgument,
		)
	}

	switch result.Result {
	case interfaces.AvailabilityPass,
		interfaces.AvailabilityFail:
		return nil

	default:
		return fmt.Errorf(
			"%w: %v",
			ErrUnknownStatus,
			result.Result,
		)
	}
}

// DispatchAvailabilityResult processes a verified AvailabilityResult off-chain.
//
// PASS:
//   - advances sequence
//   - increases cumulative payment
//   - signs PaymentState
//   - resets failure streak
//
// FAIL:
//   - advances sequence
//   - signs FailRecord
//   - increases failure streak
//   - marks SlashEligible once K is reached
//
// No on-chain transaction occurs here.
func (d *AvailabilityPaymentDispatcher) DispatchAvailabilityResult(
	result interfaces.AvailabilityResult,
) (DispatchResult, error) {

	if err := validateResult(result); err != nil {
		return DispatchResult{}, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	schedule, exists := d.schedules[result.ContractID]
	if !exists {
		return DispatchResult{}, fmt.Errorf(
			"%w: %s",
			ErrScheduleNotFound,
			result.ContractID,
		)
	}

	if schedule.ProcessedChallenges[result.ChallengeID] {
		return DispatchResult{}, fmt.Errorf(
			"%w: %s",
			ErrDuplicateChallenge,
			result.ChallengeID,
		)
	}

	if schedule.Sequence == math.MaxUint64 {
		return DispatchResult{}, ErrSequenceExhausted
	}

	nextSequence := schedule.Sequence + 1

	switch result.Result {

	case interfaces.AvailabilityPass:

		// Overflow-safe capped addition.
		nextCumulative := schedule.CurrentCumulativePayment
		remaining := schedule.MaxPayment -
			schedule.CurrentCumulativePayment

		if schedule.PaymentPerPass >= remaining {
			nextCumulative = schedule.MaxPayment
		} else {
			nextCumulative += schedule.PaymentPerPass
		}

		// Creates, signs and stores the voucher off-chain.
		signed, err := d.coordinator.CreateAndSignPaymentState(
			result,
			nextCumulative,
			nextSequence,
		)
		if err != nil {
			return DispatchResult{}, fmt.Errorf(
				"sign payment state: %w",
				err,
			)
		}

		// Commit dispatcher state only after successful signing.
		schedule.Sequence = nextSequence
		schedule.CurrentCumulativePayment = nextCumulative
		schedule.ProcessedChallenges[result.ChallengeID] = true

		// PASS breaks the failure streak.
		schedule.ConsecutiveFailures = 0
		schedule.FailStreak = nil

		return DispatchResult{
			Kind:         DispatchKindPass,
			PaymentState: signed,
		}, nil

	case interfaces.AvailabilityFail:

		// Build and sign the failure record before modifying schedule state.
		rec, err := d.buildAndSignFailRecord(
			result,
			schedule.EscrowContractID,
			nextSequence,
		)
		if err != nil {
			return DispatchResult{}, fmt.Errorf(
				"sign fail record: %w",
				err,
			)
		}

		// Compute new streak locally.
		newCount := schedule.ConsecutiveFailures

		if newCount < math.MaxUint32 {
			newCount++
		}

		newStreak := append(
			cloneFailRecords(schedule.FailStreak),
			rec,
		)

		// Keep only the most recent K records.
		if k := int(d.failureThreshold); len(newStreak) > k {
			newStreak = newStreak[len(newStreak)-k:]
		}

		// Commit after successful signing.
		schedule.Sequence = nextSequence
		schedule.ProcessedChallenges[result.ChallengeID] = true
		schedule.ConsecutiveFailures = newCount
		schedule.FailStreak = newStreak

		out := DispatchResult{
			Kind:                DispatchKindFail,
			FailRecord:          rec,
			ConsecutiveFailures: newCount,
		}

		if newCount >= d.failureThreshold {
			out.SlashEligible = true
			out.SlashEvidence = cloneFailRecords(newStreak)
		}

		return out, nil

	default:
		return DispatchResult{}, fmt.Errorf(
			"%w: %v",
			ErrUnknownStatus,
			result.Result,
		)
	}
}

// buildAndSignFailRecord constructs and signs a FailRecord.
//
// It does not modify dispatcher schedule state.
func (d *AvailabilityPaymentDispatcher) buildAndSignFailRecord(
	result interfaces.AvailabilityResult,
	escrowContractID bindings.EscrowContractID,
	sequence uint64,
) (FailRecord, error) {

	providerAddress, ok := d.coordinator.ProviderAddressFor(result)
	if !ok {
		return FailRecord{}, ErrNoProviderAddress
	}

	rec := FailRecord{
		EscrowContractID:   escrowContractID.String(),
		ProviderEthAddress: providerAddress,
		ChallengeID:        result.ChallengeID,
		Period:             result.Period,
		Sequence:           sequence,
		IssuedAt:           d.now().UTC(),
	}

	hash := sha256.Sum256(buildFailRecordPayload(rec))

	rec.PublisherSignature = ed25519.Sign(
		d.signerKey,
		hash[:],
	)

	return rec, nil
}

// buildFailRecordPayload serializes FailRecord deterministically.
func buildFailRecordPayload(rec FailRecord) []byte {
	buf := make([]byte, 0, 256)

	buf = appendString(buf, failRecordDomainTag)
	buf = appendString(buf, rec.EscrowContractID)
	buf = appendString(buf, rec.ProviderEthAddress)
	buf = appendString(buf, string(rec.ChallengeID))
	buf = appendUint64(buf, rec.Period)
	buf = appendUint64(buf, rec.Sequence)
	buf = appendInt64(buf, rec.IssuedAt.UnixNano())

	return buf
}

// VerifyFailRecord verifies a publisher-signed FailRecord.
func VerifyFailRecord(
	rec FailRecord,
	publisherPubKey ed25519.PublicKey,
) bool {

	if len(publisherPubKey) != ed25519.PublicKeySize {
		return false
	}

	if len(rec.PublisherSignature) != ed25519.SignatureSize {
		return false
	}

	hash := sha256.Sum256(buildFailRecordPayload(rec))

	return ed25519.Verify(
		publisherPubKey,
		hash[:],
		rec.PublisherSignature,
	)
}

// VerifyFailStreak verifies that records form a valid contiguous failure streak.
func VerifyFailStreak(
	records []FailRecord,
	publisherPubKey ed25519.PublicKey,
) bool {

	if len(records) == 0 {
		return false
	}

	for i, rec := range records {

		if !VerifyFailRecord(rec, publisherPubKey) {
			return false
		}

		if i == 0 {
			continue
		}

		prev := records[i-1]

		if rec.EscrowContractID != prev.EscrowContractID ||
			rec.ProviderEthAddress != prev.ProviderEthAddress ||
			prev.Sequence == math.MaxUint64 ||
			rec.Sequence != prev.Sequence+1 {
			return false
		}
	}

	return true
}

// GetContractPaymentSchedule returns a deep snapshot of the schedule.
func (d *AvailabilityPaymentDispatcher) GetContractPaymentSchedule(
	contractID availabilitytypes.ContractID,
) (PaymentSchedule, bool) {

	d.mu.Lock()
	defer d.mu.Unlock()

	schedule, ok := d.schedules[contractID]
	if !ok {
		return PaymentSchedule{}, false
	}

	snapshot := *schedule

	snapshot.ProcessedChallenges = make(
		map[availabilitytypes.ChallengeID]bool,
		len(schedule.ProcessedChallenges),
	)

	for id, processed := range schedule.ProcessedChallenges {
		snapshot.ProcessedChallenges[id] = processed
	}

	snapshot.FailStreak = cloneFailRecords(
		schedule.FailStreak,
	)

	return snapshot, true
}

// SubmitSettlementOnChain performs the full two-step on-chain settlement:
//
//  1. Submit the latest signed PaymentState on-chain (submitPaymentState).
//  2. Settle the contract (distribute funds to provider and publisher).
//
// The dispatcher itself does not create a blockchain transaction during
// challenge processing. This method is called explicitly at the end of the
// contract term.
//
// If no settler is configured on the coordinator, this falls back to
// SubmitLatestPaymentStateOnChain (step 1 only). The provider can then
// call settleContract independently.
func (d *AvailabilityPaymentDispatcher) SubmitSettlementOnChain(
	availabilityContractID availabilitytypes.ContractID,
) error {

	d.mu.Lock()

	schedule, ok := d.schedules[availabilityContractID]

	var escrowID string
	if ok {
		escrowID = schedule.EscrowContractID.String()
	}

	d.mu.Unlock()

	if !ok {
		return fmt.Errorf(
			"%w: %s",
			ErrScheduleNotFound,
			availabilityContractID,
		)
	}

	// Try the full two-step settlement flow first.
	// Falls back to step-1-only if no settler is configured.
	err := d.coordinator.SettleContractOnChain(escrowID)
	if err != nil && err.Error() == "no settler configured; call SetSettler before settlement" {
		// Fallback: submit the voucher only. The provider or another
		// party can call settleContract on the Solidity contract directly.
		return d.coordinator.SubmitLatestPaymentStateOnChain(escrowID)
	}
	return err
}

// cloneFailRecords deep-copies FailRecords and their signatures.
func cloneFailRecords(in []FailRecord) []FailRecord {
	if len(in) == 0 {
		return nil
	}

	out := make([]FailRecord, len(in))

	for i, record := range in {
		out[i] = record
		out[i].PublisherSignature = append(
			[]byte(nil),
			record.PublisherSignature...,
		)
	}

	return out
}

// appendString writes a length-prefixed string.
func appendString(buf []byte, s string) []byte {
	buf = appendUint64(buf, uint64(len(s)))
	return append(buf, s...)
}

// appendUint64 writes a big-endian uint64.
func appendUint64(buf []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(buf, v)
}

// appendInt64 writes a big-endian int64.
func appendInt64(buf []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(buf, uint64(v))
}