package transport

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, priv crypto.PrivKey, relayAddr string) (host.Host, error) {
	addr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)
	
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(addr),
		libp2p.EnableRelay(),
	}
	
	if priv != nil {
		opts = append(opts, libp2p.Identity(priv))
	}

	if relayAddr != "" {
		maddr, err := multiaddr.NewMultiaddr(relayAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid relay address: %w", err)
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("invalid relay peer info: %w", err)
		}
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{*addrInfo}))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	return h, nil
}
