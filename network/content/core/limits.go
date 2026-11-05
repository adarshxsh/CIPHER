package core

const (
	// StandardChunkSize is the normal plaintext chunk size.
	StandardChunkSize = 32 * 1024 // 32 KiB

	// ChaCha20NonceSize is the nonce size used by standard ChaCha20-Poly1305.
	ChaCha20NonceSize = 12

	// Poly1305TagSize is the authentication-tag size.
	Poly1305TagSize = 16

	// EncryptionOverhead is added to every encrypted chunk.
	EncryptionOverhead = ChaCha20NonceSize + Poly1305TagSize

	// MaxCiphertextSize is the largest encrypted chunk accepted.
	MaxCiphertextSize = StandardChunkSize + EncryptionOverhead

	// MaxManifestSize is the maximum accepted manifest size.
	MaxManifestSize = 2*1024*1024 - 3
)
