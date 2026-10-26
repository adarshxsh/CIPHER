//go:build linux || darwin

package transport

import "golang.org/x/sys/unix"

func autoCalculateFDs() int {
	var l unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &l); err == nil && l.Cur > 0 {
		fd := int(l.Cur) / 2
		if fd < 256 {
			return 256
		}
		return fd
	}
	return 256
}
