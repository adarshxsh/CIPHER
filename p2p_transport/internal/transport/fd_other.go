//go:build windows

package transport

// getFDLimit returns a default file descriptor limit for Windows systems.
func getFDLimit() int {
	return 1024
}
