// Package payment creates and manages off-chain cumulative payment states.
package payment

import (
	"errors"

	interfaces "cipher/availability/escrow-payment/interfaces"
)

type PaymentState struct {
	ContractID         string
	Publisher          string
	Provider           string
	Sequence           uint64
	Period             uint64
	CumulativePayment  uint64
	LastChallengeID    string
	Status             interfaces.AvailabilityStatus
	PublisherSignature []byte
}

// CreatePaymentState creates the cumulative payment authorization associated
// with a successful Availability result. Signing is intentionally separate.
func CreatePaymentState(result interfaces.AvailabilityResult, publisherID string, cumulativePayment, sequence uint64) (PaymentState, error) {
	if result.ContractID == "" || result.ProviderID == "" || result.ChallengeID == "" || publisherID == "" {
		return PaymentState{}, errors.New("contract ID, publisher ID, provider ID, and challenge ID are required")
	}
	if result.Result != interfaces.AvailabilityPass {
		return PaymentState{}, errors.New("payment state requires a successful availability result")
	}
	return PaymentState{
		ContractID: result.ContractID, Publisher: publisherID, Provider: result.ProviderID,
		Sequence: sequence, Period: result.Period, CumulativePayment: cumulativePayment,
		LastChallengeID: result.ChallengeID, Status: result.Result,
	}, nil
}
