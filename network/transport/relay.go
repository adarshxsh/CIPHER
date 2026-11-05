package transport

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

// DefaultRelayResources returns a relay.Resources configuration with per-peer
// active reservation caps, stream duration limits, and data transfer quotas.
func DefaultRelayResources() relay.Resources {
	rc := relay.DefaultResources()
	if rc.Limit == nil {
		rc.Limit = &relay.RelayLimit{}
	}
	rc.Limit.Duration = 2 * time.Minute
	rc.Limit.Data = 128 * 1024 // 128 KB data transfer limit per connection

	rc.MaxReservations = 128
	rc.MaxReservationsPerPeer = 1
	rc.MaxReservationsPerIP = 8
	rc.MaxReservationsPerASN = 32
	rc.MaxCircuits = 16

	return rc
}

// NewRelayService initializes a circuitv2 relay service on the provided host using relay.WithResources.
// If no custom relay options are supplied, DefaultRelayResources() is applied with relay.WithResources.
func NewRelayService(h host.Host, opts ...relay.Option) (*relay.Relay, error) {
	if len(opts) == 0 {
		opts = append(opts, relay.WithResources(DefaultRelayResources()))
	}
	r, err := relay.New(h, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate circuitv2 relay service: %w", err)
	}
	return r, nil
}
