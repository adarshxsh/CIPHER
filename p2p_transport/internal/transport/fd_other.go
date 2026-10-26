//go:build windows

package transport

// getMaxFDs returns a sensible default file descriptor count for Windows platforms.
func getMaxFDs() int {
	return 2048
}
