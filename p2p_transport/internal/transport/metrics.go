package transport

import (
	"fmt"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ResourceMetrics implements rcmgr.MetricsReporter to collect resource manager metrics safely.
type ResourceMetrics struct {
	AllowedConns   int64
	BlockedConns   int64
	AllowedStreams int64
	BlockedStreams int64
	AllowedMemory  int64
	BlockedMemory  int64
	AllowedPeers   int64
	BlockedPeers   int64
}

// NewResourceMetrics creates a new instance of ResourceMetrics.
func NewResourceMetrics() *ResourceMetrics {
	return &ResourceMetrics{}
}

func (rm *ResourceMetrics) AllowConn(dir network.Direction, usefd bool) {
	atomic.AddInt64(&rm.AllowedConns, 1)
}

func (rm *ResourceMetrics) BlockConn(dir network.Direction, usefd bool) {
	atomic.AddInt64(&rm.BlockedConns, 1)
}

func (rm *ResourceMetrics) AllowStream(p peer.ID, dir network.Direction) {
	atomic.AddInt64(&rm.AllowedStreams, 1)
}

func (rm *ResourceMetrics) BlockStream(p peer.ID, dir network.Direction) {
	atomic.AddInt64(&rm.BlockedStreams, 1)
}

func (rm *ResourceMetrics) AllowPeer(p peer.ID) {
	atomic.AddInt64(&rm.AllowedPeers, 1)
}

func (rm *ResourceMetrics) BlockPeer(p peer.ID) {
	atomic.AddInt64(&rm.BlockedPeers, 1)
}

func (rm *ResourceMetrics) AllowProtocol(proto protocol.ID)             {}
func (rm *ResourceMetrics) BlockProtocol(proto protocol.ID)             {}
func (rm *ResourceMetrics) BlockProtocolPeer(proto protocol.ID, p peer.ID) {}
func (rm *ResourceMetrics) AllowService(svc string)                     {}
func (rm *ResourceMetrics) BlockService(svc string)                     {}
func (rm *ResourceMetrics) BlockServicePeer(svc string, p peer.ID)     {}

func (rm *ResourceMetrics) AllowMemory(size int) {
	atomic.AddInt64(&rm.AllowedMemory, int64(size))
}

func (rm *ResourceMetrics) BlockMemory(size int) {
	atomic.AddInt64(&rm.BlockedMemory, int64(size))
}

func (rm *ResourceMetrics) GetAllowedStreams() int64 {
	return atomic.LoadInt64(&rm.AllowedStreams)
}

func (rm *ResourceMetrics) GetBlockedStreams() int64 {
	return atomic.LoadInt64(&rm.BlockedStreams)
}

func (rm *ResourceMetrics) GetAllowedConns() int64 {
	return atomic.LoadInt64(&rm.AllowedConns)
}

func (rm *ResourceMetrics) GetBlockedConns() int64 {
	return atomic.LoadInt64(&rm.BlockedConns)
}

func (rm *ResourceMetrics) GetAllowedMemory() int64 {
	return atomic.LoadInt64(&rm.AllowedMemory)
}

func (rm *ResourceMetrics) GetBlockedMemory() int64 {
	return atomic.LoadInt64(&rm.BlockedMemory)
}

// GetResourceStats extracts global system scope resource usage metrics for a host.
func GetResourceStats(h host.Host) (network.ScopeStat, error) {
	if h == nil || h.Network() == nil {
		return network.ScopeStat{}, fmt.Errorf("invalid host or network")
	}
	rm := h.Network().ResourceManager()
	if rm == nil {
		return network.ScopeStat{}, fmt.Errorf("resource manager not initialized on host")
	}
	var stat network.ScopeStat
	err := rm.ViewSystem(func(s network.ResourceScope) error {
		stat = s.Stat()
		return nil
	})
	return stat, err
}

// GetPeerResourceStats extracts peer scope resource usage metrics for a specific peer.
func GetPeerResourceStats(h host.Host, p peer.ID) (network.ScopeStat, error) {
	if h == nil || h.Network() == nil {
		return network.ScopeStat{}, fmt.Errorf("invalid host or network")
	}
	rm := h.Network().ResourceManager()
	if rm == nil {
		return network.ScopeStat{}, fmt.Errorf("resource manager not initialized on host")
	}
	var stat network.ScopeStat
	err := rm.ViewPeer(p, func(s network.PeerScope) error {
		stat = s.Stat()
		return nil
	})
	return stat, err
}
