package discovery

import (
	"errors"
	"net"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestValidateIP(t *testing.T) {
	tests := []struct {
		name    string
		ip      net.IP
		opts    FilterOptions
		wantErr error
	}{
		{
			name:    "Valid public IPv4",
			ip:      net.ParseIP("8.8.8.8"),
			opts:    DefaultFilterOptions,
			wantErr: nil,
		},
		{
			name:    "Valid private IPv4 under default opts",
			ip:      net.ParseIP("192.168.1.1"),
			opts:    DefaultFilterOptions,
			wantErr: nil,
		},
		{
			name:    "Private IPv4 rejected under public opts",
			ip:      net.ParseIP("192.168.1.1"),
			opts:    PublicFilterOptions,
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "Loopback IPv4 allowed under default opts",
			ip:      net.ParseIP("127.0.0.1"),
			opts:    DefaultFilterOptions,
			wantErr: nil,
		},
		{
			name:    "Loopback IPv4 rejected under public opts",
			ip:      net.ParseIP("127.0.0.1"),
			opts:    PublicFilterOptions,
			wantErr: ErrLoopbackIPNotAllowed,
		},
		{
			name:    "Unspecified IPv4 rejected",
			ip:      net.ParseIP("0.0.0.0"),
			opts:    DefaultFilterOptions,
			wantErr: ErrUnspecifiedIP,
		},
		{
			name:    "Unspecified IPv6 rejected",
			ip:      net.ParseIP("::"),
			opts:    DefaultFilterOptions,
			wantErr: ErrUnspecifiedIP,
		},
		{
			name:    "Multicast IPv4 rejected",
			ip:      net.ParseIP("224.0.0.1"),
			opts:    DefaultFilterOptions,
			wantErr: ErrMulticastIP,
		},
		{
			name:    "Bogon 0.0.0.1 rejected",
			ip:      net.ParseIP("0.0.0.1"),
			opts:    DefaultFilterOptions,
			wantErr: ErrBogonIP,
		},
		{
			name:    "Bogon CGNAT 100.64.0.1 rejected",
			ip:      net.ParseIP("100.64.0.1"),
			opts:    DefaultFilterOptions,
			wantErr: ErrBogonIP,
		},
		{
			name:    "Nil IP rejected",
			ip:      nil,
			opts:    DefaultFilterOptions,
			wantErr: ErrInvalidIP,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIP(tt.ip, tt.opts)
			if tt.wantErr != nil {
				if err == nil || !errors.Is(err, tt.wantErr) {
					t.Errorf("ValidateIP() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateIP() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateMultiaddr(t *testing.T) {
	tests := []struct {
		name    string
		addrStr string
		opts    FilterOptions
		wantErr error
	}{
		{
			name:    "Valid TCP multiaddr",
			addrStr: "/ip4/1.2.3.4/tcp/4001",
			opts:    DefaultFilterOptions,
			wantErr: nil,
		},
		{
			name:    "Valid WebSocket multiaddr",
			addrStr: "/ip4/127.0.0.1/tcp/8080/ws",
			opts:    DefaultFilterOptions,
			wantErr: nil,
		},
		{
			name:    "Unspecified multiaddr rejected",
			addrStr: "/ip4/0.0.0.0/tcp/4001",
			opts:    DefaultFilterOptions,
			wantErr: ErrUnspecifiedIP,
		},
		{
			name:    "Multicast multiaddr rejected",
			addrStr: "/ip4/224.0.0.1/tcp/4001",
			opts:    DefaultFilterOptions,
			wantErr: ErrMulticastIP,
		},
		{
			name:    "Bogon multiaddr rejected",
			addrStr: "/ip4/0.0.0.100/tcp/4001",
			opts:    DefaultFilterOptions,
			wantErr: ErrBogonIP,
		},
		{
			name:    "Zero port multiaddr rejected",
			addrStr: "/ip4/1.2.3.4/tcp/0",
			opts:    DefaultFilterOptions,
			wantErr: ErrInvalidPort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := ma.NewMultiaddr(tt.addrStr)
			if err != nil {
				t.Fatalf("Failed to parse multiaddr %s: %v", tt.addrStr, err)
			}
			vErr := ValidateMultiaddr(m, tt.opts)
			if tt.wantErr != nil {
				if vErr == nil || !errors.Is(vErr, tt.wantErr) {
					t.Errorf("ValidateMultiaddr() error = %v, wantErr %v", vErr, tt.wantErr)
				}
			} else {
				if vErr != nil {
					t.Errorf("ValidateMultiaddr() unexpected error = %v", vErr)
				}
			}
		})
	}
}

func TestFilterMultiaddrs(t *testing.T) {
	addrs := []string{
		"/ip4/1.2.3.4/tcp/4001",      // Valid
		"/ip4/0.0.0.0/tcp/4001",      // Unspecified -> dropped
		"/ip4/224.0.0.1/tcp/4001",    // Multicast -> dropped
		"/ip4/10.0.0.1/tcp/4001",     // Private -> kept under Default
		"/ip4/0.0.0.5/tcp/4001",      // Bogon -> dropped
	}

	var mAddrs []ma.Multiaddr
	for _, a := range addrs {
		m, err := ma.NewMultiaddr(a)
		if err != nil {
			t.Fatalf("Failed to parse multiaddr %s: %v", a, err)
		}
		mAddrs = append(mAddrs, m)
	}

	filtered := FilterMultiaddrs(mAddrs, DefaultFilterOptions)
	if len(filtered) != 2 {
		t.Fatalf("FilterMultiaddrs() got %d addrs, want 2", len(filtered))
	}

	expectedStr := map[string]bool{
		"/ip4/1.2.3.4/tcp/4001":  true,
		"/ip4/10.0.0.1/tcp/4001": true,
	}

	for _, m := range filtered {
		if !expectedStr[m.String()] {
			t.Errorf("Unexpected filtered address: %s", m.String())
		}
	}
}

func TestFilterProviders(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}
	dummyPID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to get peer ID from key: %v", err)
	}

	pValid := peer.AddrInfo{
		ID: dummyPID,
		Addrs: []ma.Multiaddr{
			ma.StringCast("/ip4/1.2.3.4/tcp/4001"),
		},
	}

	pPoisoned := peer.AddrInfo{
		ID: dummyPID,
		Addrs: []ma.Multiaddr{
			ma.StringCast("/ip4/0.0.0.0/tcp/4001"),
			ma.StringCast("/ip4/224.0.0.1/tcp/4001"),
		},
	}

	pEmptyID := peer.AddrInfo{
		ID: "",
		Addrs: []ma.Multiaddr{
			ma.StringCast("/ip4/1.2.3.4/tcp/4001"),
		},
	}

	providers := []peer.AddrInfo{pValid, pPoisoned, pEmptyID}

	cleaned := FilterProviders(providers, DefaultFilterOptions)
	if len(cleaned) != 1 {
		t.Fatalf("FilterProviders() returned %d providers, want 1", len(cleaned))
	}

	if cleaned[0].ID != dummyPID || len(cleaned[0].Addrs) != 1 {
		t.Errorf("Unexpected provider output: %+v", cleaned[0])
	}
}
