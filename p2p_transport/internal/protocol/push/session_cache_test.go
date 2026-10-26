package push

import (
	"crypto/rand"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func generateRandomContentID() core.ContentID {
	var id core.ContentID
	_, _ = rand.Read(id[:])
	return id
}

func TestSessionCacheBasic(t *testing.T) {
	cache := NewSessionCache(5, 10*time.Minute, 1*time.Minute)
	defer cache.Close()

	id1 := generateRandomContentID()
	sess1 := &PendingSession{ContentID: id1}

	cache.Put(sess1)
	if cache.Len() != 1 {
		t.Fatalf("expected len 1, got %d", cache.Len())
	}

	got, exists := cache.Get(id1)
	if !exists || got.ContentID != id1 {
		t.Fatalf("failed to retrieve session")
	}

	removed := cache.Remove(id1)
	if !removed || cache.Len() != 0 {
		t.Fatalf("failed to remove session")
	}

	_, exists = cache.Get(id1)
	if exists {
		t.Fatalf("expected session to be removed")
	}
}

func TestSessionCacheLRUEviction(t *testing.T) {
	cache := NewSessionCache(3, 10*time.Minute, 1*time.Minute)
	defer cache.Close()

	id1 := generateRandomContentID()
	id2 := generateRandomContentID()
	id3 := generateRandomContentID()
	id4 := generateRandomContentID()

	cache.Put(&PendingSession{ContentID: id1})
	cache.Put(&PendingSession{ContentID: id2})
	cache.Put(&PendingSession{ContentID: id3})

	if cache.Len() != 3 {
		t.Fatalf("expected len 3, got %d", cache.Len())
	}

	// Access id1 to make it MRU (most recently used)
	_, _ = cache.Get(id1)

	// Put id4, which should evict id2 (since id1 was accessed, id2 is LRU)
	cache.Put(&PendingSession{ContentID: id4})

	if cache.Len() != 3 {
		t.Fatalf("expected len 3 after eviction, got %d", cache.Len())
	}

	if _, exists := cache.Get(id2); exists {
		t.Fatalf("expected id2 (LRU) to be evicted")
	}

	if _, exists := cache.Get(id1); !exists {
		t.Fatalf("expected id1 to exist")
	}
	if _, exists := cache.Get(id3); !exists {
		t.Fatalf("expected id3 to exist")
	}
	if _, exists := cache.Get(id4); !exists {
		t.Fatalf("expected id4 to exist")
	}
}

func TestSessionCacheTTLExpiration(t *testing.T) {
	ttl := 50 * time.Millisecond
	cleanup := 20 * time.Millisecond

	cache := NewSessionCache(10, ttl, cleanup)
	defer cache.Close()

	id1 := generateRandomContentID()
	cache.Put(&PendingSession{ContentID: id1})

	if _, exists := cache.Get(id1); !exists {
		t.Fatalf("expected id1 to exist initially")
	}

	// Wait for TTL + cleanup tick
	time.Sleep(100 * time.Millisecond)

	if _, exists := cache.Get(id1); exists {
		t.Fatalf("expected id1 to be expired and evicted")
	}
	if cache.Len() != 0 {
		t.Fatalf("expected cache len 0, got %d", cache.Len())
	}
}

func TestSessionCacheConcurrency(t *testing.T) {
	cache := NewSessionCache(50, 1*time.Minute, 100*time.Millisecond)
	defer cache.Close()

	ids := make([]core.ContentID, 100)
	for i := range ids {
		ids[i] = generateRandomContentID()
	}

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(workerID int) {
			for j := 0; j < 100; j++ {
				id := ids[(workerID*10+j)%len(ids)]
				cache.Put(&PendingSession{ContentID: id})
				_, _ = cache.Get(id)
				cache.Touch(id)
				if j%5 == 0 {
					cache.Remove(id)
				}
			}
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if cache.Len() > 50 {
		t.Fatalf("expected cache len <= 50 under concurrency, got %d", cache.Len())
	}
}
