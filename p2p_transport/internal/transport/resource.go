package transport

import (
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"

	cipherProtocol "cipher/internal/protocol"
)

// DefaultScalingLimits returns a ScalingLimitConfig tuned for CIPHER nodes
// with explicit resource bounds for standard libp2p services and CIPHER protocols.
func DefaultScalingLimits() rcmgr.ScalingLimitConfig {
	limits := rcmgr.DefaultLimits

	// Apply recommended defaults for standard bundled libp2p services
	libp2p.SetDefaultServiceLimits(&limits)

	// Configure protocol limits for CIPHER Chunk Transport Protocol
	limits.AddProtocolLimit(
		cipherProtocol.ChunkTransportProtocolID,
		rcmgr.BaseLimit{
			Streams:         1024,
			StreamsInbound:  512,
			StreamsOutbound: 512,
			Conns:           512,
			ConnsInbound:    256,
			ConnsOutbound:   256,
			Memory:          128 * 1024 * 1024, // 128 MB base memory limit
		},
		rcmgr.BaseLimitIncrease{
			Streams:         256,
			StreamsInbound:  128,
			StreamsOutbound: 128,
			Memory:          64 * 1024 * 1024,
		},
	)

	// Configure protocol limits for File Transfer Protocol
	limits.AddProtocolLimit(
		cipherProtocol.FileTransferProtocolID,
		rcmgr.BaseLimit{
			Streams:         512,
			StreamsInbound:  256,
			StreamsOutbound: 256,
			Conns:           256,
			ConnsInbound:    128,
			ConnsOutbound:   128,
			Memory:          64 * 1024 * 1024, // 64 MB
		},
		rcmgr.BaseLimitIncrease{
			Streams:         128,
			StreamsInbound:  64,
			StreamsOutbound: 64,
			Memory:          32 * 1024 * 1024,
		},
	)

	// Configure service limits for CIPHER services
	limits.AddServiceLimit(
		"cipher-chunk",
		rcmgr.BaseLimit{
			Streams:         1024,
			StreamsInbound:  512,
			StreamsOutbound: 512,
			Conns:           512,
			ConnsInbound:    256,
			ConnsOutbound:   256,
			Memory:          128 * 1024 * 1024,
		},
		rcmgr.BaseLimitIncrease{
			Streams:         256,
			StreamsInbound:  128,
			StreamsOutbound: 128,
			Memory:          64 * 1024 * 1024,
		},
	)

	limits.AddServiceLimit(
		"cipher-file-transfer",
		rcmgr.BaseLimit{
			Streams:         512,
			StreamsInbound:  256,
			StreamsOutbound: 256,
			Conns:           256,
			ConnsInbound:    128,
			ConnsOutbound:   128,
			Memory:          64 * 1024 * 1024,
		},
		rcmgr.BaseLimitIncrease{
			Streams:         128,
			StreamsInbound:  64,
			StreamsOutbound: 64,
			Memory:          32 * 1024 * 1024,
		},
	)

	return limits
}

// NewResourceManager creates a new libp2p ResourceManager using the given limits.
// If limits is nil, DefaultScalingLimits() is used.
func NewResourceManager(limits *rcmgr.ScalingLimitConfig, opts ...rcmgr.Option) (network.ResourceManager, error) {
	var cfg rcmgr.ScalingLimitConfig
	if limits != nil {
		cfg = *limits
	} else {
		cfg = DefaultScalingLimits()
	}

	limiter := rcmgr.NewFixedLimiter(cfg.AutoScale())
	rm, err := rcmgr.NewResourceManager(limiter, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p resource manager: %w", err)
	}

	return rm, nil
}
