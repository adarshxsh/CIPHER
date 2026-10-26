//go:build !fault_injection

package main

import (
	"cipher/internal/protocol/chunk"
)

type faultInjectionFlags struct{}

func registerFaultInjectionFlags() *faultInjectionFlags {
	return &faultInjectionFlags{}
}

func (f *faultInjectionFlags) getOptions() []chunk.HandlerOption {
	return nil
}
