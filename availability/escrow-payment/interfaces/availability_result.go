// Package interfaces contains the boundary objects exchanged between modules.
package interfaces

import availabilitytypes "cipher/availability/availability-contracts/types"

// These aliases ensure payment consumes the exact boundary object produced by
// Availability, without duplicating its shape in another module.
type AvailabilityStatus = availabilitytypes.AvailabilityStatus

const (
	AvailabilityPass = availabilitytypes.AvailabilityPass
	AvailabilityFail = availabilitytypes.AvailabilityFail
)

type AvailabilityResult = availabilitytypes.AvailabilityResult
