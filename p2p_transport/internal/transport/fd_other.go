//go:build windows

package transport

func getFDLimit() int {
	return 1024
}
