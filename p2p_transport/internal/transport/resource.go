package transport

import (
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/pbnjay/memory"
)

// NewScalingResourceManager creates a libp2p ResourceManager auto-scaled based on system memory and file descriptor capacity.
func NewScalingResourceManager() (network.ResourceManager, error) {
	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)

	totMem := memory.TotalMemory()
	var memLimit int64
	if totMem == 0 {
		memLimit = 128 << 20 // 128 MB base libp2p memory allocation
	} else {
		memLimit = int64(totMem / 8)
		if memLimit < (128 << 20) {
			memLimit = 128 << 20
		}
	}

	fdLimit := autoCalculateFDs()

	limiter := rcmgr.NewFixedLimiter(scalingLimits.Scale(memLimit, fdLimit))
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	return rm, nil
}
