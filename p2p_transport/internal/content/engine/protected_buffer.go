package engine

import (
	"log"
	"runtime"
	"sync"
)

// ProtectedBuffer wraps a sensitive byte slice with page locking (mlock)
// and guarantees explicit memory zeroing upon destruction or release.
type ProtectedBuffer struct {
	mu     sync.RWMutex
	data   []byte
	locked bool
	closed bool
}

// NewProtectedBuffer creates a new ProtectedBuffer, copying key bytes into page-locked memory if supported.
// If mlock fails on restricted platforms, an advisory warning is logged and the buffer falls back
// to explicit memory zeroing wrappers.
func NewProtectedBuffer(key []byte) *ProtectedBuffer {
	buf := make([]byte, len(key))
	copy(buf, key)

	locked := false
	if len(buf) > 0 {
		if err := mlock(buf); err != nil {
			log.Printf("[ADVISORY] ProtectedBuffer: mlock failed (%v); falling back to explicit memory zeroing buffer", err)
		} else {
			locked = true
		}
	}

	pb := &ProtectedBuffer{
		data:   buf,
		locked: locked,
		closed: false,
	}

	runtime.SetFinalizer(pb, func(p *ProtectedBuffer) {
		p.Destroy()
	})

	return pb
}

// Destroy zeroes out the underlying memory buffer, unlocks the locked memory pages if locked,
// and marks the buffer as closed.
func (pb *ProtectedBuffer) Destroy() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.closed {
		return
	}

	if len(pb.data) > 0 {
		Wipe(pb.data)
		if pb.locked {
			_ = munlock(pb.data)
			pb.locked = false
		}
	}

	pb.closed = true
}

// Bytes returns the underlying key bytes if the buffer is active, or nil if closed.
func (pb *ProtectedBuffer) Bytes() []byte {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	if pb.closed {
		return nil
	}
	return pb.data
}

// IsClosed returns true if the buffer has been destroyed.
func (pb *ProtectedBuffer) IsClosed() bool {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	return pb.closed
}

// IsZero returns true if the buffer is closed or all underlying bytes are zero.
func (pb *ProtectedBuffer) IsZero() bool {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	if pb.data == nil {
		return true
	}
	for _, b := range pb.data {
		if b != 0 {
			return false
		}
	}
	return true
}

// IsLocked returns true if the buffer memory pages are locked.
func (pb *ProtectedBuffer) IsLocked() bool {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	return pb.locked
}
