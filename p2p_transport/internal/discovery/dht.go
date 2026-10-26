package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type blankValidator struct{}

func (blankValidator) Validate(key string, value []byte) error { return nil }
func (blankValidator) Select(key string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return -1, fmt.Errorf("no values")
	}
	return 0, nil
}

// ProviderRecordValidator validates provider records stored under the "/provider/" namespace in the DHT.
type ProviderRecordValidator struct{}

func (v ProviderRecordValidator) Validate(key string, value []byte) error {
	var record SignedProviderRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return fmt.Errorf("invalid provider record JSON: %w", err)
	}
	return VerifyProviderRecord(&record)
}

func (v ProviderRecordValidator) Select(key string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return -1, fmt.Errorf("no values to select from")
	}

	bestIdx := -1
	var maxExpiry int64 = -1

	for i, val := range values {
		var record SignedProviderRecord
		if err := json.Unmarshal(val, &record); err != nil {
			continue
		}
		if err := VerifyProviderRecord(&record); err != nil {
			continue
		}
		if record.ExpiryTimestamp > maxExpiry {
			maxExpiry = record.ExpiryTimestamp
			bestIdx = i
		}
	}

	if bestIdx == -1 {
		return 0, nil
	}
	return bestIdx, nil
}

// NewDHT creates and returns a Kademlia DHT bound to the given host.
// mode should be dht.ModeServer for peers (they help route/store records too,
// matching your "providers" box — everyone participates).
func NewDHT(h host.Host, mode dht.ModeOpt) (*dht.IpfsDHT, error) {
	v := record.NamespacedValidator{
		"pk":       record.PublicKeyValidator{},
		"ipns":     blankValidator{},
		"provider": &ProviderRecordValidator{},
	}
	kdht, err := dht.New(h, dht.Mode(mode), dht.ProtocolPrefix("/cipher"), dht.Validator(v))

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
