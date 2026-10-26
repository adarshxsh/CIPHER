package discovery

import (
	"context"
	"fmt"
	"log"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// DHTOption configures settings on a new Kademlia DHT instance.
type DHTOption func(*dhtConfig)

// Option is an alias for DHTOption for convenience.
type Option = DHTOption

type dhtConfig struct {
	queryConcurrency  int
	routingTableLimit int
	dhtOpts           []dht.Option
}

// WithQueryConcurrency configures the maximum query concurrency for DHT operations.
func WithQueryConcurrency(c int) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.queryConcurrency = c
	}
}

// WithRoutingTableLimit configures the maximum routing table capacity (k-bucket size) for the DHT.
func WithRoutingTableLimit(limit int) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.routingTableLimit = limit
	}
}

// WithBucketSize configures the routing table k-bucket size for the DHT.
func WithBucketSize(size int) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.routingTableLimit = size
	}
}

// WithDHTOption passes a raw go-libp2p-kad-dht Option to dht.New.
func WithDHTOption(opt dht.Option) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.dhtOpts = append(cfg.dhtOpts, opt)
	}
}

// NewDHT creates and returns a Kademlia DHT bound to the given host.
// mode should be dht.ModeServer for peers (they help route/store records too,
// matching your "providers" box — everyone participates).
func NewDHT(h host.Host, mode dht.ModeOpt, opts ...DHTOption) (*dht.IpfsDHT, error) {
	cfg := &dhtConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	dhtOptions := []dht.Option{dht.Mode(mode)}

	if cfg.queryConcurrency > 0 {
		dhtOptions = append(dhtOptions, dht.Concurrency(cfg.queryConcurrency))
	}

	if cfg.routingTableLimit > 0 {
		dhtOptions = append(dhtOptions, dht.BucketSize(cfg.routingTableLimit))
	}

	dhtOptions = append(dhtOptions, cfg.dhtOpts...)

	kdht, err := dht.New(h, dhtOptions...)

	if err != nil {
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}

	return kdht, nil
}

// Bootstrap connects to a set of known-good peers and runs the DHT's
// self-bootstrap routine so the routing table starts filling up.
func Bootstrap(ctx context.Context, kdht *dht.IpfsDHT, h host.Host, seeds []peer.AddrInfo) error {
	if len(seeds) == 0 {
		return fmt.Errorf("no bootstrap seeds provided")
	}

	connected := 0

	for _, seed := range seeds {
		if seed.ID == h.ID() {
			continue // Skip self
		}

		log.Printf(
			"[DHT] Connecting to bootstrap peer %s...",
			seed.ID,
		)

		if err := h.Connect(ctx, seed); err != nil {
			log.Printf(
				"[DHT] Failed to connect to bootstrap peer %s: %v",
				seed.ID,
				err,
			)
			continue
		}

		log.Printf(
			"[DHT] Connected to bootstrap peer %s",
			seed.ID,
		)

		connected++
	}

	if connected == 0 {
		return fmt.Errorf("failed to connect to any bootstrap seed")
	}

	// Give the newly established connections a moment to settle.
	time.Sleep(500 * time.Millisecond)

	log.Printf("[DHT] Starting DHT bootstrap...")

	// So this is where the control shifts to the DHT's internal bootstrap routine, which will populate the routing table.
	// Below is the flowchart for what this function actually does
	if err := kdht.Bootstrap(ctx); err != nil {
		return fmt.Errorf("DHT bootstrap failed: %w", err)
	}

	log.Printf(
		"[DHT] Bootstrap complete; routing table has %d peers",
		len(kdht.RoutingTable().ListPeers()),
	)

	return nil
}

// kdht.Bootstrap(ctx)
//         │
//         ▼
// Kademlia routing-table refresh
//         │
//         ▼
// query peers
//         │
//         ▼
// learn about peers closer to target
//         │
//         ▼
// A appears as a discovered peer
//         │
//         ▼
// DHT query needs to contact A
//         │
//         ▼
// libp2p DHT query code dials A
//         │
//         ▼
// libp2p Host
//         │
//         ▼
// Network connection established
