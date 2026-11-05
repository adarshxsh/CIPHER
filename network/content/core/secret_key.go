package core

import (
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
)

var ErrKeyDestroyed = errors.New("secret key has been destroyed")

// SecretKey wraps sensitive key bytes, pins memory using unix.Mlock where supported,
// and enforces explicit key wiping via Destroy().
type SecretKey struct {
	mu        sync.RWMutex
	bytes     []byte
	pinned    bool
	destroyed bool
}

// NewSecretKey creates a new SecretKey initialized with a copy of data.
func NewSecretKey(data []byte) (*SecretKey, error) {
	if len(data) == 0 {
		return nil, errors.New("key data cannot be empty")
	}

	b := make([]byte, len(data))
	copy(b, data)

	pinned := false
	if err := unix.Mlock(b); err == nil {
		pinned = true
	}

	return &SecretKey{
		bytes:  b,
		pinned: pinned,
	}, nil
}

// NewRandomSecretKey creates a new SecretKey initialized with cryptographically secure random bytes.
func NewRandomSecretKey(size int) (*SecretKey, error) {
	if size <= 0 {
		return nil, errors.New("invalid key size")
	}

	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("failed to generate random key: %w", err)
	}

	pinned := false
	if err := unix.Mlock(b); err == nil {
		pinned = true
	}

	return &SecretKey{
		bytes:  b,
		pinned: pinned,
	}, nil
}

// Bytes returns the raw key buffer or ErrKeyDestroyed if the key has been wiped.
func (s *SecretKey) Bytes() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.destroyed || s.bytes == nil {
		return nil, ErrKeyDestroyed
	}
	return s.bytes, nil
}

// Destroy zeroizes the key buffer explicitly and releases memory locks.
func (s *SecretKey) Destroy() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.destroyed {
		return
	}

	if s.bytes != nil {
		zeros := make([]byte, len(s.bytes))
		subtle.ConstantTimeCopy(1, s.bytes, zeros)
		for i := range s.bytes {
			s.bytes[i] = 0
		}

		if s.pinned {
			_ = unix.Munlock(s.bytes)
			s.pinned = false
		}
		s.bytes = nil
	}

	s.destroyed = true
}

// IsDestroyed returns whether Destroy() has been called on this key handle.
func (s *SecretKey) IsDestroyed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.destroyed
}

// IsPinned returns whether the underlying key memory is currently locked with mlock.
func (s *SecretKey) IsPinned() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.pinned
}

// Len returns the length of the secret key in bytes.
func (s *SecretKey) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.destroyed || s.bytes == nil {
		return 0
	}
	return len(s.bytes)
}

// Clone creates an independent SecretKey instance with a copy of the key data in a new pinned buffer.
func (s *SecretKey) Clone() (*SecretKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.destroyed || s.bytes == nil {
		return nil, ErrKeyDestroyed
	}
	return NewSecretKey(s.bytes)
}
