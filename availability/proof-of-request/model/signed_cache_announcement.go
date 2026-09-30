package model

type SignedCacheAnnouncement struct {
	Announcement CacheAnnouncement
	Signature    []byte
}