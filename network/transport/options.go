package transport

import (
	"time"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

type nodeOptions struct {
	partialLimits   *rcmgr.PartialLimitConfig
	metricsReporter rcmgr.MetricsReporter
	resourceManager network.ResourceManager
	connManager     connmgr.ConnManager
	maxMemory       int64
	lowWatermark    int
	highWatermark   int
	gracePeriod     time.Duration
}

// NodeOption configures options for libp2p host creation.
type NodeOption func(*nodeOptions)

// WithPartialLimitConfig provides custom resource manager limit overrides.
func WithPartialLimitConfig(limits rcmgr.PartialLimitConfig) NodeOption {
	return func(o *nodeOptions) {
		o.partialLimits = &limits
	}
}

// WithMemoryLimit sets custom total memory limit in bytes.
func WithMemoryLimit(mem int64) NodeOption {
	return func(o *nodeOptions) {
		o.maxMemory = mem
	}
}

// WithConnLimits sets custom low and high watermarks and grace period for connection manager.
func WithConnLimits(low, high int, grace time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.lowWatermark = low
		o.highWatermark = high
		o.gracePeriod = grace
	}
}

// WithMetricsReporter configures a custom resource manager metrics reporter.
func WithMetricsReporter(reporter rcmgr.MetricsReporter) NodeOption {
	return func(o *nodeOptions) {
		o.metricsReporter = reporter
	}
}

// WithResourceManager injects a pre-built network.ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(o *nodeOptions) {
		o.resourceManager = rm
	}
}

// WithConnManager injects a pre-built connmgr.ConnManager.
func WithConnManager(cm connmgr.ConnManager) NodeOption {
	return func(o *nodeOptions) {
		o.connManager = cm
	}
}

func applyResourceLimits(base *rcmgr.ResourceLimits, user rcmgr.ResourceLimits) {
	if user.Streams != 0 {
		base.Streams = user.Streams
	}
	if user.StreamsInbound != 0 {
		base.StreamsInbound = user.StreamsInbound
	}
	if user.StreamsOutbound != 0 {
		base.StreamsOutbound = user.StreamsOutbound
	}
	if user.Conns != 0 {
		base.Conns = user.Conns
	}
	if user.ConnsInbound != 0 {
		base.ConnsInbound = user.ConnsInbound
	}
	if user.ConnsOutbound != 0 {
		base.ConnsOutbound = user.ConnsOutbound
	}
	if user.FD != 0 {
		base.FD = user.FD
	}
	if user.Memory != 0 {
		base.Memory = user.Memory
	}
}

func applyPartialLimitOverrides(base *rcmgr.PartialLimitConfig, user rcmgr.PartialLimitConfig) {
	applyResourceLimits(&base.System, user.System)
	applyResourceLimits(&base.Transient, user.Transient)
	applyResourceLimits(&base.AllowlistedSystem, user.AllowlistedSystem)
	applyResourceLimits(&base.AllowlistedTransient, user.AllowlistedTransient)
	applyResourceLimits(&base.ServiceDefault, user.ServiceDefault)
	applyResourceLimits(&base.ServicePeerDefault, user.ServicePeerDefault)
	applyResourceLimits(&base.ProtocolDefault, user.ProtocolDefault)
	applyResourceLimits(&base.ProtocolPeerDefault, user.ProtocolPeerDefault)
	applyResourceLimits(&base.PeerDefault, user.PeerDefault)
	applyResourceLimits(&base.Conn, user.Conn)
	applyResourceLimits(&base.Stream, user.Stream)

	if user.Service != nil {
		if base.Service == nil {
			base.Service = make(map[string]rcmgr.ResourceLimits)
		}
		for k, v := range user.Service {
			cur := base.Service[k]
			applyResourceLimits(&cur, v)
			base.Service[k] = cur
		}
	}
	if user.ServicePeer != nil {
		if base.ServicePeer == nil {
			base.ServicePeer = make(map[string]rcmgr.ResourceLimits)
		}
		for k, v := range user.ServicePeer {
			cur := base.ServicePeer[k]
			applyResourceLimits(&cur, v)
			base.ServicePeer[k] = cur
		}
	}
	if user.Protocol != nil {
		if base.Protocol == nil {
			base.Protocol = make(map[protocol.ID]rcmgr.ResourceLimits)
		}
		for k, v := range user.Protocol {
			cur := base.Protocol[k]
			applyResourceLimits(&cur, v)
			base.Protocol[k] = cur
		}
	}
	if user.ProtocolPeer != nil {
		if base.ProtocolPeer == nil {
			base.ProtocolPeer = make(map[protocol.ID]rcmgr.ResourceLimits)
		}
		for k, v := range user.ProtocolPeer {
			cur := base.ProtocolPeer[k]
			applyResourceLimits(&cur, v)
			base.ProtocolPeer[k] = cur
		}
	}
	if user.Peer != nil {
		if base.Peer == nil {
			base.Peer = make(map[peer.ID]rcmgr.ResourceLimits)
		}
		for k, v := range user.Peer {
			cur := base.Peer[k]
			applyResourceLimits(&cur, v)
			base.Peer[k] = cur
		}
	}
}
