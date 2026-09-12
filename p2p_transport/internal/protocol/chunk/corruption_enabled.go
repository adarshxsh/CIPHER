//go:build corrupt_test

package chunk

import (
	"log"
	"math/rand"
)

// WithTestCorruption configures the probability of chunk corruption for testing when built with 'corrupt_test'.
func WithTestCorruption(prob float64) HandlerOption {
	return func(cfg *HandlerConfig) {
		if prob > 1.0 {
			prob = 1.0
		} else if prob < 0.0 {
			prob = 0.0
		}
		cfg.CorruptProb = prob
	}
}

// maybeCorruptChunk applies test corruption based on the StreamHandler instance configuration.
func maybeCorruptChunk(h *StreamHandler, chunkID [32]byte, data []byte) []byte {
	if h.cfg.CorruptProb > 0 && rand.Float64() < h.cfg.CorruptProb && len(data) > 0 {
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		corrupted := make([]byte, len(data))
		copy(corrupted, data)
		corrupted[0] ^= 0xFF
		return corrupted
	}
	return data
}
