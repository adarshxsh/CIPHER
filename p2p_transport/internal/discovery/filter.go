package discovery

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var (
	ErrInvalidIP            = errors.New("invalid or empty IP address")
	ErrUnspecifiedIP        = errors.New("unspecified IP address (0.0.0.0 or ::)")
	ErrMulticastIP          = errors.New("multicast IP address not allowed")
	ErrBogonIP              = errors.New("bogon or reserved unroutable IP address")
	ErrLoopbackIPNotAllowed = errors.New("loopback IP address not allowed in current filter options")
	ErrPrivateIPNotAllowed  = errors.New("private IP address not allowed in current filter options")
	ErrLinkLocalIPNotAllowed= errors.New("link-local IP address not allowed in current filter options")
	ErrInvalidMultiaddr     = errors.New("nil or invalid multiaddr")
	ErrInvalidPort          = errors.New("invalid port number (must be 1-65535)")
	ErrEmptyDNSHost         = errors.New("empty domain name in DNS multiaddr")
)

// FilterOptions configures multiaddress range filtering and IP validation rules.
type FilterOptions struct {
	AllowPrivate      bool
	AllowLoopback     bool
	AllowLinkLocal    bool
	RejectUnspecified bool
	RejectMulticast   bool
	RejectBogon       bool
}

// DefaultFilterOptions provides filtering suitable for standard P2P networks
// (permitting local testing/LAN while strictly rejecting invalid/unspecified/multicast/bogon addresses).
var DefaultFilterOptions = FilterOptions{
	AllowPrivate:      true,
	AllowLoopback:     true,
	AllowLinkLocal:    false,
	RejectUnspecified: true,
	RejectMulticast:   true,
	RejectBogon:       true,
}

// PublicFilterOptions provides strict filtering for public internet deployments
// (rejecting private, loopback, link-local, unspecified, multicast, and bogon addresses).
var PublicFilterOptions = FilterOptions{
	AllowPrivate:      false,
	AllowLoopback:     false,
	AllowLinkLocal:    false,
	RejectUnspecified: true,
	RejectMulticast:   true,
	RejectBogon:       true,
}

var bogonNets []*net.IPNet

func init() {
	bogonCIDRs := []string{
		"0.0.0.0/8",          // "This host on this network"
		"240.0.0.0/4",        // Reserved for future use (Class E)
		"255.255.255.255/32", // Limited broadcast
		"100.64.0.0/10",      // Carrier-grade NAT (Unroutable on public internet)
		"192.0.0.0/24",       // IETF Protocol Assignments
		"192.0.2.0/24",       // TEST-NET-1
		"198.51.100.0/24",    // TEST-NET-2
		"203.0.113.0/24",     // TEST-NET-3
		"198.18.0.0/15",      // Benchmarking
	}
	for _, cidr := range bogonCIDRs {
		_, ipnet, err := net.ParseCIDR(cidr)
		if err == nil {
			bogonNets = append(bogonNets, ipnet)
		}
	}
}

// ValidateIP checks an IP address against the specified FilterOptions.
func ValidateIP(ip net.IP, opts FilterOptions) error {
	if len(ip) == 0 {
		return ErrInvalidIP
	}

	if opts.RejectUnspecified && ip.IsUnspecified() {
		return ErrUnspecifiedIP
	}

	if opts.RejectMulticast && ip.IsMulticast() {
		return ErrMulticastIP
	}

	if opts.RejectBogon {
		for _, bNet := range bogonNets {
			if bNet.Contains(ip) {
				return fmt.Errorf("%w: %s in %s", ErrBogonIP, ip.String(), bNet.String())
			}
		}
	}

	if ip.IsLoopback() {
		if !opts.AllowLoopback {
			return ErrLoopbackIPNotAllowed
		}
		return nil
	}

	if isPrivateIP(ip) {
		if !opts.AllowPrivate {
			return ErrPrivateIPNotAllowed
		}
		return nil
	}

	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		if !opts.AllowLinkLocal {
			return ErrLinkLocalIPNotAllowed
		}
		return nil
	}

	return nil
}

func isPrivateIP(ip net.IP) bool {
	privateCIDRs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fd00::/8",
	}
	for _, cidr := range privateCIDRs {
		_, ipnet, err := net.ParseCIDR(cidr)
		if err == nil && ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

// ValidateMultiaddr validates a multiaddress structure, its IP components, and protocol components.
func ValidateMultiaddr(m ma.Multiaddr, opts FilterOptions) error {
	if m == nil {
		return ErrInvalidMultiaddr
	}

	if manet.IsIPUnspecified(m) && opts.RejectUnspecified {
		return ErrUnspecifiedIP
	}

	ip, err := manet.ToIP(m)
	if err == nil && ip != nil {
		if err := ValidateIP(ip, opts); err != nil {
			return err
		}
	}

	// Inspect multiaddr protocol components (ports, DNS names, etc.)
	var componentErr error
	ma.ForEach(m, func(c ma.Component) bool {
		switch c.Protocol().Code {
		case ma.P_TCP, ma.P_UDP:
			portStr := c.Value()
			port, parseErr := strconv.Atoi(portStr)
			if parseErr != nil || port <= 0 || port > 65535 {
				componentErr = fmt.Errorf("%w: %s", ErrInvalidPort, portStr)
				return false
			}
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6, ma.P_DNSADDR:
			if c.Value() == "" {
				componentErr = ErrEmptyDNSHost
				return false
			}
		}
		return true
	})

	return componentErr
}

// FilterMultiaddrs filters a slice of multiaddresses, removing any that fail validation under opts.
func FilterMultiaddrs(addrs []ma.Multiaddr, opts FilterOptions) []ma.Multiaddr {
	if len(addrs) == 0 {
		return nil
	}
	filtered := make([]ma.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		if err := ValidateMultiaddr(addr, opts); err == nil {
			filtered = append(filtered, addr)
		}
	}
	return filtered
}

// FilterAddrInfo filters a peer's AddrInfo, retaining only multiaddresses that pass validation.
func FilterAddrInfo(ai peer.AddrInfo, opts FilterOptions) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    ai.ID,
		Addrs: FilterMultiaddrs(ai.Addrs, opts),
	}
}

// FilterProviders filters a slice of provider AddrInfo structs, stripping invalid multiaddrs
// and dropping providers that have no valid multiaddresses or empty Peer ID.
func FilterProviders(providers []peer.AddrInfo, opts FilterOptions) []peer.AddrInfo {
	if len(providers) == 0 {
		return nil
	}
	filtered := make([]peer.AddrInfo, 0, len(providers))
	for _, p := range providers {
		if p.ID == "" {
			continue
		}
		cleaned := FilterAddrInfo(p, opts)
		if len(cleaned.Addrs) > 0 {
			filtered = append(filtered, cleaned)
		}
	}
	return filtered
}
