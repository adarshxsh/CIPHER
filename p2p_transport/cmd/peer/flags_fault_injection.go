//go:build fault_injection

package main

import (
	"flag"
	"log"

	"cipher/internal/protocol/chunk"
)

type faultInjectionFlags struct {
	corruptProb *float64
}

func registerFaultInjectionFlags() *faultInjectionFlags {
	return &faultInjectionFlags{
		corruptProb: flag.Float64("test-corrupt-prob", 0.0, "Probability (0.0 to 1.0) of sending a corrupt chunk for testing"),
	}
}

func (f *faultInjectionFlags) getOptions() []chunk.HandlerOption {
	if f != nil && f.corruptProb != nil && *f.corruptProb > 0 {
		log.Printf("[TESTING] Chunk corruption probability set to %.2f", *f.corruptProb)
		return []chunk.HandlerOption{chunk.WithCorruptProbability(*f.corruptProb)}
	}
	return nil
}
