//go:build !corrupt_test

package chunk

import "log"

// WithTestCorruption is a no-op when the 'corrupt_test' build tag is not set.
func WithTestCorruption(prob float64) HandlerOption {
	return func(cfg *HandlerConfig) {
		if prob > 0 {
			log.Printf("[WARNING] Test corruption requested (%.2f) but corrupt_test build tag is disabled.", prob)
		}
	}
}

// maybeCorruptChunk returns chunk data unmodified in standard builds.
func maybeCorruptChunk(h *StreamHandler, chunkID [32]byte, data []byte) []byte {
	return data
}
