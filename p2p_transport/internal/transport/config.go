package transport

import (
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

// Config holds transport and resource management configuration options.
type Config struct {
	MaxTotalConns     int
	MaxPeerStreams    int
	MaxMemoryBytes    int64
	ConnLowWatermark  int
	ConnHighWatermark int
	ConnGracePeriod   time.Duration

	ResourceManager network.ResourceManager
	Limiter         rcmgr.Limiter
	PartialLimits   *rcmgr.PartialLimitConfig
	RcmgrOpts       []rcmgr.Option
}

// DefaultConfig returns default transport settings with resource limits:
// 100 max total connections, 16 max streams per peer.
func DefaultConfig() Config {
	return Config{
		MaxTotalConns:     100,
		MaxPeerStreams:    16,
		MaxMemoryBytes:    0,
		ConnLowWatermark:  50,
		ConnHighWatermark: 200,
		ConnGracePeriod:   time.Minute,
	}
}

// NodeOption configures transport host creation.
type NodeOption func(*Config)

// WithConnManager configures connection manager low/high watermarks and grace period.
func WithConnManager(low, high int, grace time.Duration) NodeOption {
	return func(c *Config) {
		c.ConnLowWatermark = low
		c.ConnHighWatermark = high
		c.ConnGracePeriod = grace
	}
}

// WithResourceManager sets a custom pre-configured libp2p ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(c *Config) {
		c.ResourceManager = rm
	}
}

// WithConnLimits sets the maximum total peer connections.
func WithConnLimits(maxConns int) NodeOption {
	return func(c *Config) {
		c.MaxTotalConns = maxConns
	}
}

// WithPeerStreamLimit sets the maximum streams allowed per peer.
func WithPeerStreamLimit(maxStreams int) NodeOption {
	return func(c *Config) {
		c.MaxPeerStreams = maxStreams
	}
}

// WithMemoryLimit sets the maximum system memory limit in bytes.
func WithMemoryLimit(maxMemory int64) NodeOption {
	return func(c *Config) {
		c.MaxMemoryBytes = maxMemory
	}
}

// WithPartialLimits specifies a partial limit config to override default limits.
func WithPartialLimits(pl *rcmgr.PartialLimitConfig) NodeOption {
	return func(c *Config) {
		c.PartialLimits = pl
	}
}

// WithRcmgrLimiter sets a custom rcmgr.Limiter.
func WithRcmgrLimiter(limiter rcmgr.Limiter) NodeOption {
	return func(c *Config) {
		c.Limiter = limiter
	}
}
