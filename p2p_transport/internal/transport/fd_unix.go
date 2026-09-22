//go:build !windows

package transport

import "syscall"

// getMaxFDs returns the maximum number of file descriptors allowed by the system soft limit.
func getMaxFDs() int {
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err == nil && rlim.Cur > 0 {
		return int(rlim.Cur)
	}
	return 1024
}
