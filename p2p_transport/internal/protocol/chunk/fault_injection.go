//go:build fault_injection

package chunk

import (
	"log"
	"math/rand"

	"cipher/internal/content/core"
)

type HandlerOption func(*StreamHandler)

// WithCorruptProbability returns a HandlerOption that sets the corruption probability for testing.
func WithCorruptProbability(prob float64) HandlerOption {
	return func(h *StreamHandler) {
		h.SetCorruptProbability(prob)
	}
}

// WithFaultInjector returns a HandlerOption with a custom fault injection function.
func WithFaultInjector(fn func(data *core.Chunk) *core.Chunk) HandlerOption {
	return func(h *StreamHandler) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.faultInjector = fn
	}
}

// SetCorruptProbability sets the chunk corruption probability in a thread-safe manner.
func (h *StreamHandler) SetCorruptProbability(prob float64) {
	if prob < 0 {
		prob = 0
	} else if prob > 1 {
		prob = 1
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.corruptProb = prob
}

// GetCorruptProbability returns the chunk corruption probability.
func (h *StreamHandler) GetCorruptProbability() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.corruptProb
}

func (h *StreamHandler) applyFaultInjection(chunkData *core.Chunk) *core.Chunk {
	if chunkData == nil {
		return nil
	}

	h.mu.RLock()
	injector := h.faultInjector
	prob := h.corruptProb
	h.mu.RUnlock()

	if injector != nil {
		return injector(chunkData)
	}

	if prob > 0 && rand.Float64() < prob && len(chunkData.Data) > 0 {
		log.Printf("[TESTING] Corrupting chunk %x", chunkData.Header.ID)

		// Deep copy the chunk payload data byte slice to prevent mutating origin buffers in storage engines
		clonedData := make([]byte, len(chunkData.Data))
		copy(clonedData, chunkData.Data)
		clonedData[0] ^= 0xFF

		clonedChunk := *chunkData
		clonedChunk.Data = clonedData
		return &clonedChunk
	}

	return chunkData
}
