package transport

import (
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
)

// NewHost creates a new libp2p host with the given private key.
// By default, it listens on all network interfaces on a random port.
// For Phase 1, we are just standing up a bare libp2p host.
func NewHost(privKey crypto.PrivKey, listenPort int) (host.Host, error) {
	// If listenPort is 0, libp2p will choose a random available port.
	listenAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)

	// Create the libp2p node
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(listenAddr),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	return h, nil
}
