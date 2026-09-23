package crypto

import "runtime"

// Zeroize overwrites all bytes in the given slice with 0 and invokes runtime.KeepAlive
// to prevent compiler dead-code elimination from optimizing away the zeroing loop.
func Zeroize(b []byte) {
	if len(b) == 0 {
		return
	}
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}
