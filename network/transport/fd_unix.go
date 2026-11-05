//go:build !windows

package transport

import (
	"syscall"
)

// getFDLimit returns the soft file descriptor limit for the process,
// or a default fallback if the limit cannot be determined.
func getFDLimit() int {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
		if rLimit.Cur > 0 {
			return int(rLimit.Cur)
		}
	}
	return 1024
}
