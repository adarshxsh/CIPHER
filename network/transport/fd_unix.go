//go:build !windows

package transport

import (
	"golang.org/x/sys/unix"
)

func getFDLimit() int {
	var rlim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rlim); err == nil && rlim.Cur > 0 {
		return int(rlim.Cur)
	}
	return 1024
}
