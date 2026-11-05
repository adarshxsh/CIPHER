package discovery

import (
	"context"
	"os"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// IsPrivateDHTAllowed returns true if environment variables permit private or local IP addresses in the DHT.
func IsPrivateDHTAllowed() bool {
	privDHT := strings.ToLower(os.Getenv("CIPHER_ALLOW_PRIVATE_DHT"))
	localIP := strings.ToLower(os.Getenv("CIPHER_ALLOW_LOCAL_IP"))
	return privDHT == "1" || privDHT == "true" || localIP == "1" || localIP == "true"
}

// IsRoutableAddress returns true if multiaddress a is considered routable.
// In public mode, loopback, unspecified, and private addresses are rejected.
// In private network mode (when CIPHER_ALLOW_PRIVATE_DHT=1 or CIPHER_ALLOW_LOCAL_IP=1),
// loopback and private subnets are permitted, while unspecified addresses (0.0.0.0, ::) are still rejected.
func IsRoutableAddress(a ma.Multiaddr) bool {
	if a == nil {
		return false
	}
	if manet.IsIPUnspecified(a) {
		return false
	}
	if IsPrivateDHTAllowed() {
		return true
	}
	return manet.IsPublicAddr(a)
}

// PublicAddressFilter filters a slice of multiaddresses, keeping only routable addresses.
func PublicAddressFilter(addrs []ma.Multiaddr) []ma.Multiaddr {
	return ma.FilterAddrs(addrs, IsRoutableAddress)
}

// SanitizeAddrInfo returns a copy of peer.AddrInfo with unroutable multiaddresses removed.
func SanitizeAddrInfo(info peer.AddrInfo) peer.AddrInfo {
	info.Addrs = ma.FilterAddrs(info.Addrs, IsRoutableAddress)
	return info
}

// SanitizeProviderChannel wraps an incoming channel of peer.AddrInfo,
// yielding sanitized peer.AddrInfo structs with non-empty routable multiaddress lists.
func SanitizeProviderChannel(ctx context.Context, in <-chan peer.AddrInfo) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case p, ok := <-in:
				if !ok {
					return
				}
				sanitized := SanitizeAddrInfo(p)
				if len(sanitized.Addrs) > 0 {
					select {
					case <-ctx.Done():
						return
					case out <- sanitized:
					}
				}
			}
		}
	}()
	return out
}
