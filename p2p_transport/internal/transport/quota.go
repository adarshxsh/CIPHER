package transport

import (
	"time"
)

// NodeRole defines standard node roles in the transport network.
type NodeRole string

const (
	RoleClient    NodeRole = "client"
	RoleProvider  NodeRole = "provider"
	RoleRelay     NodeRole = "relay"
	RoleBootstrap NodeRole = "bootstrap"
)

// QuotaConfig defines connection manager watermarks and resource manager limits for a host.
type QuotaConfig struct {
	// Connection Manager Watermarks
	ConnLow     int           `json:"conn_low"`
	ConnHigh    int           `json:"conn_high"`
	GracePeriod time.Duration `json:"grace_period"`

	// Resource Manager Limits
	MaxMemory          int64 `json:"max_memory"`
	MaxStreams         int   `json:"max_streams"`
	MaxStreamsInbound  int   `json:"max_streams_inbound"`
	MaxStreamsOutbound int   `json:"max_streams_outbound"`
	MaxConns           int   `json:"max_conns"`
	MaxConnsInbound    int   `json:"max_conns_inbound"`
	MaxConnsOutbound   int   `json:"max_conns_outbound"`
}

// DefaultQuotaConfig returns default baseline quota configuration maintaining backward compatibility.
func DefaultQuotaConfig() QuotaConfig {
	return QuotaConfig{
		ConnLow:            20,
		ConnHigh:           50,
		GracePeriod:        20 * time.Second,
		MaxMemory:          256 * 1024 * 1024, // 256MB
		MaxStreams:         512,
		MaxStreamsInbound:  256,
		MaxStreamsOutbound: 256,
		MaxConns:           100,
		MaxConnsInbound:    50,
		MaxConnsOutbound:   50,
	}
}

// ClientQuotaProfile returns quota settings tuned for client nodes.
func ClientQuotaProfile() QuotaConfig {
	return QuotaConfig{
		ConnLow:            10,
		ConnHigh:           25,
		GracePeriod:        15 * time.Second,
		MaxMemory:          128 * 1024 * 1024, // 128MB
		MaxStreams:         256,
		MaxStreamsInbound:  64,
		MaxStreamsOutbound: 192,
		MaxConns:           40,
		MaxConnsInbound:    10,
		MaxConnsOutbound:   30,
	}
}

// ProviderQuotaProfile returns quota settings tuned for provider nodes.
func ProviderQuotaProfile() QuotaConfig {
	return QuotaConfig{
		ConnLow:            100,
		ConnHigh:           200,
		GracePeriod:        30 * time.Second,
		MaxMemory:          512 * 1024 * 1024, // 512MB
		MaxStreams:         2048,
		MaxStreamsInbound:  1024,
		MaxStreamsOutbound: 1024,
		MaxConns:           300,
		MaxConnsInbound:    200,
		MaxConnsOutbound:   100,
	}
}

// RelayQuotaProfile returns quota settings tuned for relay nodes.
func RelayQuotaProfile() QuotaConfig {
	return QuotaConfig{
		ConnLow:            400,
		ConnHigh:           800,
		GracePeriod:        1 * time.Minute,
		MaxMemory:          1024 * 1024 * 1024, // 1GB
		MaxStreams:         4096,
		MaxStreamsInbound:  2048,
		MaxStreamsOutbound: 2048,
		MaxConns:           1000,
		MaxConnsInbound:    500,
		MaxConnsOutbound:   500,
	}
}

// BootstrapQuotaProfile returns quota settings tuned for bootstrap nodes.
func BootstrapQuotaProfile() QuotaConfig {
	return QuotaConfig{
		ConnLow:            500,
		ConnHigh:           1000,
		GracePeriod:        1 * time.Minute,
		MaxMemory:          1024 * 1024 * 1024, // 1GB
		MaxStreams:         4096,
		MaxStreamsInbound:  2048,
		MaxStreamsOutbound: 2048,
		MaxConns:           1200,
		MaxConnsInbound:    800,
		MaxConnsOutbound:   400,
	}
}

// GetRoleQuotaConfig returns the predefined QuotaConfig for a specified NodeRole.
func GetRoleQuotaConfig(role NodeRole) QuotaConfig {
	switch role {
	case RoleClient:
		return ClientQuotaProfile()
	case RoleProvider:
		return ProviderQuotaProfile()
	case RoleRelay:
		return RelayQuotaProfile()
	case RoleBootstrap:
		return BootstrapQuotaProfile()
	default:
		return DefaultQuotaConfig()
	}
}

type nodeOptions struct {
	quotaConfig QuotaConfig
}

// NodeOption configures parameters for host creation.
type NodeOption func(*nodeOptions)

// WithQuotaConfig specifies a custom QuotaConfig struct.
func WithQuotaConfig(cfg QuotaConfig) NodeOption {
	return func(o *nodeOptions) {
		o.quotaConfig = cfg
	}
}

// WithRoleProfile sets quota limits based on predefined role profiles.
func WithRoleProfile(role NodeRole) NodeOption {
	return func(o *nodeOptions) {
		o.quotaConfig = GetRoleQuotaConfig(role)
	}
}

// WithConnWatermarks overrides connection manager watermarks.
func WithConnWatermarks(low, high int, grace time.Duration) NodeOption {
	return func(o *nodeOptions) {
		if low > 0 {
			o.quotaConfig.ConnLow = low
		}
		if high > 0 {
			o.quotaConfig.ConnHigh = high
		}
		if grace > 0 {
			o.quotaConfig.GracePeriod = grace
		}
	}
}

// WithResourceLimits overrides resource manager limits.
func WithResourceLimits(maxMemory int64, maxStreams int, maxConns int) NodeOption {
	return func(o *nodeOptions) {
		if maxMemory > 0 {
			o.quotaConfig.MaxMemory = maxMemory
		}
		if maxStreams > 0 {
			o.quotaConfig.MaxStreams = maxStreams
		}
		if maxConns > 0 {
			o.quotaConfig.MaxConns = maxConns
		}
	}
}
