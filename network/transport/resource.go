package transport

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/multiformats/go-multiaddr"
)

// NodeRole specifies operational profile modes for resource allocation.
type NodeRole string

const (
	RoleDefault   NodeRole = "default"
	RolePeer      NodeRole = "peer"
	RoleBootstrap NodeRole = "bootstrap"
	RoleRelay     NodeRole = "relay"
	RoleClient    NodeRole = "client"
	RolePublisher NodeRole = "publisher"
	RoleProvider  NodeRole = "provider"
)

// ParseNodeRole parses a string into a validated NodeRole.
func ParseNodeRole(s string) (NodeRole, error) {
	trimmed := strings.ToLower(strings.TrimSpace(s))
	if trimmed == "" {
		return RoleDefault, nil
	}
	switch NodeRole(trimmed) {
	case RoleDefault, RolePeer:
		return RoleDefault, nil
	case RoleBootstrap:
		return RoleBootstrap, nil
	case RoleRelay:
		return RoleRelay, nil
	case RoleClient:
		return RoleClient, nil
	case RolePublisher:
		return RolePublisher, nil
	case RoleProvider:
		return RoleProvider, nil
	default:
		return "", fmt.Errorf("invalid node role: %q", s)
	}
}

// ResourceConfig defines resource manager parameters for transport host initialization.
type ResourceConfig struct {
	Role NodeRole `json:"role,omitempty"`

	MaxMemory int64 `json:"max_memory,omitempty"`
	MaxConns  int   `json:"max_conns,omitempty"`
	MaxFD     int   `json:"max_fd,omitempty"`

	InboundConns  int `json:"inbound_conns,omitempty"`
	OutboundConns int `json:"outbound_conns,omitempty"`

	PeerStreamLimit int `json:"peer_stream_limit,omitempty"`
	PeerConnLimit   int `json:"peer_conn_limit,omitempty"`

	JSONConfigPath string `json:"json_config_path,omitempty"`

	ProtocolLimits map[protocol.ID]rcmgr.BaseLimit `json:"protocol_limits,omitempty"`

	Allowlist []multiaddr.Multiaddr `json:"-"`
}

// DefaultProfileConfig returns resource configuration defaults for a given role profile.
func DefaultProfileConfig(role NodeRole) ResourceConfig {
	switch role {
	case RoleBootstrap:
		return ResourceConfig{
			Role:            RoleBootstrap,
			MaxMemory:       2 * 1024 * 1024 * 1024, // 2 GB
			MaxConns:        2000,
			InboundConns:    1500,
			OutboundConns:   500,
			MaxFD:           4096,
			PeerConnLimit:   32,
			PeerStreamLimit: 512,
		}
	case RoleRelay:
		return ResourceConfig{
			Role:            RoleRelay,
			MaxMemory:       1 * 1024 * 1024 * 1024, // 1 GB
			MaxConns:        1000,
			InboundConns:    800,
			OutboundConns:   200,
			MaxFD:           2048,
			PeerConnLimit:   16,
			PeerStreamLimit: 256,
		}
	case RoleClient:
		return ResourceConfig{
			Role:            RoleClient,
			MaxMemory:       128 * 1024 * 1024, // 128 MB
			MaxConns:        30,
			InboundConns:    10,
			OutboundConns:   20,
			MaxFD:           256,
			PeerConnLimit:   8,
			PeerStreamLimit: 64,
		}
	default:
		return ResourceConfig{
			Role:            RoleDefault,
			MaxMemory:       512 * 1024 * 1024, // 512 MB
			MaxConns:        200,
			InboundConns:    150,
			OutboundConns:   50,
			MaxFD:           1024,
			PeerConnLimit:   16,
			PeerStreamLimit: 128,
		}
	}
}

// ParseMemoryString converts human-readable memory strings (e.g. "512MB", "1GB", "256MiB", "1073741824")
// or numeric byte strings into int64 bytes.
func ParseMemoryString(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	upper := strings.ToUpper(s)
	unitMultiplier := int64(1)

	if strings.HasSuffix(upper, "GIB") || strings.HasSuffix(upper, "GB") || strings.HasSuffix(upper, "G") {
		unitMultiplier = 1024 * 1024 * 1024
		upper = strings.TrimRight(upper, "GIB")
	} else if strings.HasSuffix(upper, "MIB") || strings.HasSuffix(upper, "MB") || strings.HasSuffix(upper, "M") {
		unitMultiplier = 1024 * 1024
		upper = strings.TrimRight(upper, "MIB")
	} else if strings.HasSuffix(upper, "KIB") || strings.HasSuffix(upper, "KB") || strings.HasSuffix(upper, "K") {
		unitMultiplier = 1024
		upper = strings.TrimRight(upper, "KIB")
	} else if strings.HasSuffix(upper, "B") {
		upper = strings.TrimSuffix(upper, "B")
	}

	upper = strings.TrimSpace(upper)
	for _, r := range upper {
		if !unicode.IsDigit(r) {
			return 0, fmt.Errorf("invalid memory limit %q: non-numeric characters", s)
		}
	}

	val, err := strconv.ParseInt(upper, 10, 64)
	if err != nil || val <= 0 {
		return 0, fmt.Errorf("invalid memory limit %q: must be a positive integer", s)
	}

	return val * unitMultiplier, nil
}

// LoadResourceConfigFromEnv parses resource limits from environment variables:
// CIPHER_ROLE, CIPHER_MAX_MEMORY, CIPHER_MAX_CONNS, CIPHER_MAX_FD (or CIPHER_MAX_FDS),
// and CIPHER_RESOURCE_CONFIG (or CIPHER_RCMGR_CONFIG / CIPHER_LIMITS_FILE).
func LoadResourceConfigFromEnv() (ResourceConfig, error) {
	roleEnv := os.Getenv("CIPHER_ROLE")
	role, err := ParseNodeRole(roleEnv)
	if err != nil {
		return ResourceConfig{}, err
	}

	cfg := DefaultProfileConfig(role)

	if memStr := os.Getenv("CIPHER_MAX_MEMORY"); memStr != "" {
		memBytes, err := ParseMemoryString(memStr)
		if err != nil {
			return ResourceConfig{}, err
		}
		cfg.MaxMemory = memBytes
	}

	if connsStr := os.Getenv("CIPHER_MAX_CONNS"); connsStr != "" {
		conns, err := strconv.Atoi(strings.TrimSpace(connsStr))
		if err != nil || conns <= 0 {
			return ResourceConfig{}, fmt.Errorf("invalid CIPHER_MAX_CONNS %q: must be a positive integer", connsStr)
		}
		cfg.MaxConns = conns
		cfg.InboundConns = int(float64(conns) * 0.75)
		cfg.OutboundConns = conns - cfg.InboundConns
	}

	fdStr := os.Getenv("CIPHER_MAX_FD")
	if fdStr == "" {
		fdStr = os.Getenv("CIPHER_MAX_FDS")
	}
	if fdStr != "" {
		fds, err := strconv.Atoi(strings.TrimSpace(fdStr))
		if err != nil || fds <= 0 {
			return ResourceConfig{}, fmt.Errorf("invalid CIPHER_MAX_FD %q: must be a positive integer", fdStr)
		}
		cfg.MaxFD = fds
	}

	jsonPath := os.Getenv("CIPHER_RESOURCE_CONFIG")
	if jsonPath == "" {
		jsonPath = os.Getenv("CIPHER_RCMGR_CONFIG")
	}
	if jsonPath == "" {
		jsonPath = os.Getenv("CIPHER_LIMITS_FILE")
	}
	if jsonPath != "" {
		if _, err := os.Stat(jsonPath); err != nil {
			return ResourceConfig{}, fmt.Errorf("resource config JSON file not found at %s: %w", jsonPath, err)
		}
		cfg.JSONConfigPath = jsonPath
	}

	if err := cfg.Validate(); err != nil {
		return ResourceConfig{}, err
	}

	return cfg, nil
}

// Validate ensures resource bounds are positive and consistent.
func (cfg ResourceConfig) Validate() error {
	if cfg.MaxMemory < 0 {
		return fmt.Errorf("MaxMemory cannot be negative: %d", cfg.MaxMemory)
	}
	if cfg.MaxConns < 0 {
		return fmt.Errorf("MaxConns cannot be negative: %d", cfg.MaxConns)
	}
	if cfg.MaxFD < 0 {
		return fmt.Errorf("MaxFD cannot be negative: %d", cfg.MaxFD)
	}
	if cfg.JSONConfigPath != "" {
		if _, err := os.Stat(cfg.JSONConfigPath); err != nil {
			return fmt.Errorf("JSONConfigPath invalid or inaccessible: %w", err)
		}
	}
	return nil
}

// applyCipherProtocolLimits configures protocol-specific resource bounds for Cipher wire protocols.
func applyCipherProtocolLimits(cfg *rcmgr.ScalingLimitConfig, role NodeRole) {
	baseStreams := 256
	baseMemory := int64(64 * 1024 * 1024)

	switch role {
	case RoleBootstrap:
		baseStreams = 1024
		baseMemory = 256 * 1024 * 1024
	case RoleRelay:
		baseStreams = 512
		baseMemory = 128 * 1024 * 1024
	case RoleClient:
		baseStreams = 64
		baseMemory = 16 * 1024 * 1024
	}

	chunkProto := protocol.ID("/cipher/chunk/1.0.0")
	fileProto := protocol.ID("/cipher/filetransfer/1.0.0")
	provideProto := protocol.ID("/cipher/dht/provide/1.0.0")
	ipfsIdProto := protocol.ID("/ipfs/id/1.0.0")
	ipfsKadProto := protocol.ID("/ipfs/kad/1.0.0")

	protos := []protocol.ID{chunkProto, fileProto, provideProto, ipfsIdProto, ipfsKadProto}

	for _, p := range protos {
		cfg.AddProtocolLimit(
			p,
			rcmgr.BaseLimit{
				StreamsInbound:  baseStreams,
				StreamsOutbound: baseStreams,
				Streams:         baseStreams * 2,
				Memory:          baseMemory,
			},
			rcmgr.BaseLimitIncrease{
				StreamsInbound:  baseStreams / 2,
				StreamsOutbound: baseStreams / 2,
				Streams:         baseStreams,
				Memory:          baseMemory / 2,
			},
		)
	}
}

// BuildResourceManager creates a network.ResourceManager configured according to ResourceConfig settings.
func (cfg ResourceConfig) BuildResourceManager() (network.ResourceManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ResourceConfig: %w", err)
	}

	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)

	if cfg.MaxConns > 0 {
		inbound := cfg.InboundConns
		outbound := cfg.OutboundConns
		if inbound == 0 && outbound == 0 {
			inbound = int(float64(cfg.MaxConns) * 0.75)
			outbound = cfg.MaxConns - inbound
		}
		scalingLimits.SystemBaseLimit.Conns = cfg.MaxConns
		scalingLimits.SystemBaseLimit.ConnsInbound = inbound
		scalingLimits.SystemBaseLimit.ConnsOutbound = outbound
	}

	if cfg.MaxMemory > 0 {
		scalingLimits.SystemBaseLimit.Memory = cfg.MaxMemory
	}

	if cfg.MaxFD > 0 {
		scalingLimits.SystemBaseLimit.FD = cfg.MaxFD
	}

	if cfg.PeerConnLimit > 0 {
		scalingLimits.PeerBaseLimit.Conns = cfg.PeerConnLimit
		scalingLimits.PeerBaseLimit.ConnsInbound = cfg.PeerConnLimit
		scalingLimits.PeerBaseLimit.ConnsOutbound = cfg.PeerConnLimit
	}

	if cfg.PeerStreamLimit > 0 {
		scalingLimits.PeerBaseLimit.Streams = cfg.PeerStreamLimit
		scalingLimits.PeerBaseLimit.StreamsInbound = cfg.PeerStreamLimit
		scalingLimits.PeerBaseLimit.StreamsOutbound = cfg.PeerStreamLimit
	}

	applyCipherProtocolLimits(&scalingLimits, cfg.Role)

	for proto, baseLimit := range cfg.ProtocolLimits {
		scalingLimits.AddProtocolLimit(proto, baseLimit, rcmgr.BaseLimitIncrease{})
	}

	mem := cfg.MaxMemory
	if mem <= 0 {
		mem = 512 * 1024 * 1024
	}
	fds := cfg.MaxFD
	if fds <= 0 {
		fds = getFDLimit()
	}

	concreteLimits := scalingLimits.Scale(mem, fds)

	var limiter rcmgr.Limiter

	if cfg.JSONConfigPath != "" {
		f, err := os.Open(cfg.JSONConfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open resource config JSON file %s: %w", cfg.JSONConfigPath, err)
		}
		defer f.Close()

		jsonLimiter, err := rcmgr.NewLimiterFromJSON(f, concreteLimits)
		if err != nil {
			return nil, fmt.Errorf("failed to parse resource config JSON: %w", err)
		}
		limiter = jsonLimiter
	} else {
		limiter = rcmgr.NewFixedLimiter(concreteLimits)
	}

	var opts []rcmgr.Option
	if len(cfg.Allowlist) > 0 {
		opts = append(opts, rcmgr.WithAllowlistedMultiaddrs(cfg.Allowlist))
	}

	rm, err := rcmgr.NewResourceManager(limiter, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	return rm, nil
}
