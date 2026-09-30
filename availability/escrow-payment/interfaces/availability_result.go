// Package interfaces contains the boundary objects exchanged between modules.
package interfaces

import "time"

type AvailabilityStatus string

const (
	AvailabilityPass AvailabilityStatus = "PASS"
	AvailabilityFail AvailabilityStatus = "FAIL"
)

// AvailabilityResult is produced by Availability after a provider response is
// verified. Payment consumes it without needing proof or network details.
type AvailabilityResult struct {
	ContractID  string
	ProviderID  string
	Period      uint64
	ChallengeID string
	Result      AvailabilityStatus
	Timestamp   time.Time
}
