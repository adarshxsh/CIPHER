package transport

import (
	"fmt"
	"time"

	connmgr "github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/network"
)

// NodeOption is a functional option for configuring host resource limits and connection parameters.
type NodeOption func(*nodeConfig) error

type nodeConfig struct {
	connLow     int
	connHigh    int
	connGrace   time.Duration
	connMgr     connmgr.ConnManager
	memoryLimit int64
	resourceMgr network.ResourceManager
}

func defaultConfig() *nodeConfig {
	return &nodeConfig{
		connLow:     50,
		connHigh:    150,
		connGrace:   time.Minute,
		memoryLimit: 1 << 30, // Default 1 GB memory limit
	}
}

// WithConnLimits configures connection manager low/high watermarks and grace period.
func WithConnLimits(low, high int, grace time.Duration) NodeOption {
	return func(cfg *nodeConfig) error {
		if low < 0 || high < low {
			return fmt.Errorf("invalid connection limits: low=%d, high=%d", low, high)
		}
		cfg.connLow = low
		cfg.connHigh = high
		cfg.connGrace = grace
		return nil
	}
}

// WithMemoryLimit configures custom system memory limit in bytes for the resource manager.
func WithMemoryLimit(bytes int64) NodeOption {
	return func(cfg *nodeConfig) error {
		if bytes <= 0 {
			return fmt.Errorf("invalid memory limit: %d", bytes)
		}
		cfg.memoryLimit = bytes
		return nil
	}
}

// WithResourceManager overrides the default resource manager with a custom implementation.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(cfg *nodeConfig) error {
		if rm == nil {
			return fmt.Errorf("resource manager cannot be nil")
		}
		cfg.resourceMgr = rm
		return nil
	}
}

// WithConnManager overrides the connection manager with a custom implementation.
func WithConnManager(cm connmgr.ConnManager) NodeOption {
	return func(cfg *nodeConfig) error {
		if cm == nil {
			return fmt.Errorf("connection manager cannot be nil")
		}
		cfg.connMgr = cm
		return nil
	}
}
