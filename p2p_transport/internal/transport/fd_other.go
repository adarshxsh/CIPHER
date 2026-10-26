//go:build !linux && !darwin

package transport

func autoCalculateFDs() int {
	return 256
}
