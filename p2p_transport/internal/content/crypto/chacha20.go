package crypto

// ChaCha20Encryptor is an alias for XChaCha20Encryptor.
type ChaCha20Encryptor = XChaCha20Encryptor

// NewChaCha20Encryptor returns a new XChaCha20Encryptor.
func NewChaCha20Encryptor() *XChaCha20Encryptor {
	return NewXChaCha20Encryptor()
}

