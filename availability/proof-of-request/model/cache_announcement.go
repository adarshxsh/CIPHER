package model

import "time"

type CacheAnnouncement struct {
	ProviderID string
	FileID     string
	MerkleRoot string
	Version    uint64
	Expiry     time.Time
}