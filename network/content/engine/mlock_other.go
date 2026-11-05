//go:build !(unix || linux || darwin || freebsd || openbsd || netbsd)

package engine

func mlock(b []byte) error {
	return nil
}

func munlock(b []byte) error {
	return nil
}
