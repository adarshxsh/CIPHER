package discovery

import (
	"context"
	"os"
	"strings"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/libp2p/go-libp2p/core/peer"
)

// isPrivateDHTAllowed checks if private/local multiaddresses are permitted for DHT operations
// (e.g. during local integration tests or development).
func isPrivateDHTAllowed() bool {
	v1 := strings.ToLower(strings.TrimSpace(os.Getenv("CIPHER_ALLOW_PRIVATE_DHT")))
	if v1 == "1" || v1 == "true" {
		return true
	}
	v2 := strings.ToLower(strings.TrimSpace(os.Getenv("CIPHER_ALLOW_LOCAL_IP")))
	if v2 == "1" || v2 == "true" {
		return true
	}
	return false
}

// IsRoutableAddress determines if a multiaddress is valid and routable for DHT routing table insertion.
// Unspecified addresses (0.0.0.0 or ::) are always rejected.
// When private DHT is enabled via environment variables, loopback and private subnets are permitted.
// Otherwise, only publicly routable addresses (as reported by manet.IsPublicAddr) are accepted.
func IsRoutableAddress(addr ma.Multiaddr) bool {
	if addr == nil {
		return false
	}

	if manet.IsIPUnspecified(addr) {
		return false
	}

	if isPrivateDHTAllowed() {
		return true
	}

	return manet.IsPublicAddr(addr)
}

// PublicAddressFilter filters a slice of multiaddresses, keeping only routable multiaddresses.
func PublicAddressFilter(addrs []ma.Multiaddr) []ma.Multiaddr {
	return ma.FilterAddrs(addrs, IsRoutableAddress)
}

// SanitizeAddrInfo returns a copy of peer.AddrInfo with unroutable multiaddresses filtered out.
func SanitizeAddrInfo(info peer.AddrInfo) peer.AddrInfo {
	info.Addrs = PublicAddressFilter(info.Addrs)
	return info
}

// SanitizeProviderChannel wraps an incoming channel of peer.AddrInfo and yields
// peer.AddrInfo values with non-routable multiaddresses stripped out.
func SanitizeProviderChannel(ctx context.Context, in <-chan peer.AddrInfo) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case info, ok := <-in:
				if !ok {
					return
				}
				sanitized := SanitizeAddrInfo(info)
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
