package core

import (
	"errors"
	"runtime"
	"sync"
)

var ErrKeyClosed = errors.New("key handle is closed")

// ZeroBytes overwrites every byte in b with 0x00 in a way that prevents compiler optimizations from stripping the write.
func ZeroBytes(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
	if len(b) > 0 {
		runtime.KeepAlive(&b[0])
	}
	runtime.KeepAlive(b)
}

// KeyHandle is a secure container wrapping secret key bytes with finalizer-backed automatic zeroing on GC or explicit Close.
type KeyHandle struct {
	mu     sync.RWMutex
	b      []byte
	closed bool
}

// NewKeyHandle creates a new KeyHandle wrapping a copy of the provided key bytes.
// It registers a GC finalizer to zero the key memory if Close is not explicitly called.
func NewKeyHandle(key []byte) *KeyHandle {
	if key == nil {
		return &KeyHandle{closed: true}
	}
	b := make([]byte, len(key))
	copy(b, key)
	kh := &KeyHandle{b: b}
	runtime.SetFinalizer(kh, func(h *KeyHandle) {
		h.Close()
	})
	return kh
}

// Bytes returns the underlying key bytes. Returns nil if closed.
func (kh *KeyHandle) Bytes() []byte {
	kh.mu.RLock()
	defer kh.mu.RUnlock()
	if kh.closed {
		return nil
	}
	return kh.b
}

// Close explicitly zeroes the underlying key bytes and releases the key handle.
func (kh *KeyHandle) Close() error {
	kh.mu.Lock()
	defer kh.mu.Unlock()
	if kh.closed {
		return nil
	}
	kh.closed = true
	ZeroBytes(kh.b)
	kh.b = nil
	runtime.SetFinalizer(kh, nil)
	return nil
}

// IsClosed returns true if the key handle has been closed/zeroed.
func (kh *KeyHandle) IsClosed() bool {
	kh.mu.RLock()
	defer kh.mu.RUnlock()
	return kh.closed
}
