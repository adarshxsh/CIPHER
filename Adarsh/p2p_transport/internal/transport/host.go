package transport

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
)

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, priv crypto.PrivKey) (host.Host, error) {
	addr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)
	
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(addr),
	}
	
	if priv != nil {
		opts = append(opts, libp2p.Identity(priv))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	return h, nil
}
