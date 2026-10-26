//go:build !fault_injection

package chunk

import (
	"cipher/internal/content/core"
)

type HandlerOption func(*StreamHandler)

// WithCorruptProbability is a no-op option in production builds.
func WithCorruptProbability(prob float64) HandlerOption {
	return func(h *StreamHandler) {}
}

func (h *StreamHandler) applyFaultInjection(chunkData *core.Chunk) *core.Chunk {
	return chunkData
}
