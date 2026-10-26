package discovery

import (
	"context"
	"os"
	"strings"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// IsPublicAddr checks if a multiaddr is a valid, publicly routable address.
// It returns false for loopback (127.0.0.1, ::1), private IPs (10.x, 192.168.x, 172.16-31.x, fd00::/8),
// link-local (169.254.x, fe80::/10), and unspecified (0.0.0.0, ::) addresses.
func IsPublicAddr(a multiaddr.Multiaddr) bool {
	if a == nil {
		return false
	}
	if manet.IsIPLoopback(a) || manet.IsPrivateAddr(a) || manet.IsIP6LinkLocal(a) || manet.IsIPUnspecified(a) {
		return false
	}
	return manet.IsPublicAddr(a)
}

// IsPrivateOrLoopbackAddr returns true if the given address is loopback, private, link-local, or unspecified.
func IsPrivateOrLoopbackAddr(a multiaddr.Multiaddr) bool {
	if a == nil {
		return true
	}
	return manet.IsIPLoopback(a) || manet.IsPrivateAddr(a) || manet.IsIP6LinkLocal(a) || manet.IsIPUnspecified(a)
}

// FilterAddresses filters a slice of multiaddresses according to public/private routing policy.
// If allowPrivate is false, only publicly routable multiaddresses are retained.
func FilterAddresses(addrs []multiaddr.Multiaddr, allowPrivate bool) []multiaddr.Multiaddr {
	if allowPrivate {
		return addrs
	}
	var valid []multiaddr.Multiaddr
	for _, a := range addrs {
		if IsPublicAddr(a) {
			valid = append(valid, a)
		}
	}
	return valid
}

// PublicAddressFilter is a dht.AddressFilter function that strips non-public multiaddresses
// (loopback, private IP, link-local, unspecified) prior to peerstore insertion in public DHT mode.
func PublicAddressFilter(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	if IsPrivateNetworkAllowed() {
		return addrs
	}
	return FilterAddresses(addrs, false)
}

// CustomPublicRoutingTableFilter validates peer connectedness and network reachability
// for routing table entries in public DHT mode.
func CustomPublicRoutingTableFilter(dhtObj interface{}, p peer.ID) bool {
	return dht.PublicRoutingTableFilter(dhtObj, p)
}

// SanitizeAddrInfo returns a copy of peer.AddrInfo with unroutable or private multiaddresses removed.
func SanitizeAddrInfo(info peer.AddrInfo, allowPrivate bool) peer.AddrInfo {
	info.Addrs = FilterAddresses(info.Addrs, allowPrivate)
	return info
}

// SanitizeProviderChannel filters provider query results from an async channel,
// dropping unroutable multiaddresses and omitting peers that have no valid addresses remaining.
func SanitizeProviderChannel(ctx context.Context, in <-chan peer.AddrInfo, allowPrivate bool) <-chan peer.AddrInfo {
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
				sanitized := SanitizeAddrInfo(p, allowPrivate)
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

// IsPrivateNetworkAllowed checks if private/local network configurations are permitted
// via environment flag CIPHER_ALLOW_PRIVATE_DHT.
func IsPrivateNetworkAllowed() bool {
	env := strings.ToLower(os.Getenv("CIPHER_ALLOW_PRIVATE_DHT"))
	return env == "1" || env == "true" || env == "yes" || env == "private"
}
