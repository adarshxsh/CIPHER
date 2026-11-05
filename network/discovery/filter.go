package discovery

import (
	"context"
	"log"
	"os"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// IsAllowPrivateDHT checks whether private/loopback IP multiaddresses are allowed in DHT discovery.
// Enabled via environment variables CIPHER_ALLOW_LOCAL_IP=1 or CIPHER_ALLOW_PRIVATE_DHT=1.
func IsAllowPrivateDHT() bool {
	return os.Getenv("CIPHER_ALLOW_LOCAL_IP") == "1" || os.Getenv("CIPHER_ALLOW_PRIVATE_DHT") == "1"
}

// IsRoutableAddress checks if a multiaddress is valid and routable based on the local/LAN mode setting.
// In public (WAN) mode (allowPrivate == false):
//   - Loopback (127.0.0.0/8, ::1), RFC1918 private subnets, link-local, and unspecified (0.0.0.0, ::) are rejected.
//   - Only publicly routable addresses return true.
// In private (LAN) mode (allowPrivate == true):
//   - Loopback and private IP addresses are allowed.
//   - Unspecified IPs (0.0.0.0, ::) and invalid multiaddrs are rejected.
func IsRoutableAddress(addr ma.Multiaddr, allowPrivate bool) bool {
	if addr == nil {
		return false
	}

	if manet.IsIPUnspecified(addr) {
		return false
	}

	if allowPrivate {
		return true
	}

	if manet.IsIPLoopback(addr) || manet.IsPrivateAddr(addr) || !manet.IsPublicAddr(addr) {
		return false
	}

	return true
}

// PublicAddressFilter is a libp2p dht.AddressFilter function that filters out non-routable addresses.
func PublicAddressFilter(addrs []ma.Multiaddr) []ma.Multiaddr {
	return FilterAddresses(addrs, IsAllowPrivateDHT())
}

// FilterAddresses filters a slice of multiaddresses according to the routability rules.
func FilterAddresses(addrs []ma.Multiaddr, allowPrivate bool) []ma.Multiaddr {
	return FilterAddressesForPeer("", addrs, allowPrivate)
}

// FilterAddressesForPeer filters multiaddresses for a specific peer, logging rejected addresses for auditing.
func FilterAddressesForPeer(p peer.ID, addrs []ma.Multiaddr, allowPrivate bool) []ma.Multiaddr {
	if len(addrs) == 0 {
		return nil
	}

	valid := make([]ma.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		if IsRoutableAddress(addr, allowPrivate) {
			valid = append(valid, addr)
		} else {
			if p != "" {
				log.Printf("[DHT Address Filter] Rejected multiaddr %s from peer %s (allowPrivate=%v)", addr, p, allowPrivate)
			} else {
				log.Printf("[DHT Address Filter] Rejected multiaddr %s (allowPrivate=%v)", addr, allowPrivate)
			}
		}
	}
	return valid
}

// SanitizeAddrInfo filters non-routable addresses from a peer.AddrInfo struct.
func SanitizeAddrInfo(info peer.AddrInfo, allowPrivate bool) peer.AddrInfo {
	info.Addrs = FilterAddressesForPeer(info.ID, info.Addrs, allowPrivate)
	return info
}

// SanitizeProviderChannel wraps an incoming channel of peer.AddrInfo and returns a channel
// yielding sanitized peer.AddrInfo structs with non-routable multiaddrs stripped.
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
				} else {
					log.Printf("[DHT Provider Filter] Discarded provider %s with no routable multiaddresses", p.ID)
				}
			}
		}
	}()
	return out
}
