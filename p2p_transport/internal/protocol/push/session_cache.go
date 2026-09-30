package push

import (
	"container/list"
	"sync"
	"time"

	"cipher/internal/content/core"
)

const (
	DefaultMaxSessions     = 100
	DefaultSessionTTL      = 15 * time.Minute
	DefaultCleanupInterval = 1 * time.Minute
)

type lruEntry struct {
	contentID core.ContentID
	session   *PendingSession
}

type SessionCache struct {
	mu              sync.Mutex
	capacity        int
	ttl             time.Duration
	cleanupInterval time.Duration

	items     map[core.ContentID]*list.Element
	evictList *list.List

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewSessionCache(capacity int, ttl time.Duration, cleanupInterval time.Duration) *SessionCache {
	if capacity <= 0 {
		capacity = DefaultMaxSessions
	}
	if ttl <= 0 {
		ttl = DefaultSessionTTL
	}
	if cleanupInterval <= 0 {
		cleanupInterval = DefaultCleanupInterval
	}

	sc := &SessionCache{
		capacity:        capacity,
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
		items:           make(map[core.ContentID]*list.Element),
		evictList:       list.New(),
		stopCh:          make(chan struct{}),
	}

	sc.wg.Add(1)
	go sc.cleanupLoop()

	return sc
}

func (sc *SessionCache) Get(id core.ContentID) (*PendingSession, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	elem, exists := sc.items[id]
	if !exists {
		return nil, false
	}

	entry := elem.Value.(*lruEntry)
	if sc.ttl > 0 && time.Since(entry.session.UpdatedAt) > sc.ttl {
		sc.removeElement(elem)
		return nil, false
	}

	sc.evictList.MoveToFront(elem)
	entry.session.UpdatedAt = time.Now()
	return entry.session, true
}

func (sc *SessionCache) Put(session *PendingSession) {
	if session == nil {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	now := time.Now()
	session.UpdatedAt = now

	if elem, exists := sc.items[session.ContentID]; exists {
		sc.evictList.MoveToFront(elem)
		elem.Value.(*lruEntry).session = session
		return
	}

	// Evict oldest if capacity reached
	for sc.evictList.Len() >= sc.capacity {
		sc.removeOldest()
	}

	entry := &lruEntry{
		contentID: session.ContentID,
		session:   session,
	}
	elem := sc.evictList.PushFront(entry)
	sc.items[session.ContentID] = elem
}

func (sc *SessionCache) Touch(id core.ContentID) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if elem, exists := sc.items[id]; exists {
		sc.evictList.MoveToFront(elem)
		elem.Value.(*lruEntry).session.UpdatedAt = time.Now()
	}
}

func (sc *SessionCache) Remove(id core.ContentID) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if elem, exists := sc.items[id]; exists {
		sc.removeElement(elem)
		return true
	}
	return false
}

func (sc *SessionCache) removeOldest() {
	elem := sc.evictList.Back()
	if elem != nil {
		sc.removeElement(elem)
	}
}

func (sc *SessionCache) removeElement(elem *list.Element) {
	sc.evictList.Remove(elem)
	entry := elem.Value.(*lruEntry)
	delete(sc.items, entry.contentID)
}

func (sc *SessionCache) Len() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.evictList.Len()
}

func (sc *SessionCache) Close() {
	sc.mu.Lock()
	select {
	case <-sc.stopCh:
		// already closed
		sc.mu.Unlock()
		return
	default:
		close(sc.stopCh)
	}
	sc.mu.Unlock()

	sc.wg.Wait()
}

func (sc *SessionCache) cleanupLoop() {
	defer sc.wg.Done()

	ticker := time.NewTicker(sc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sc.stopCh:
			return
		case <-ticker.C:
			sc.evictExpired()
		}
	}
}

func (sc *SessionCache) evictExpired() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.ttl <= 0 {
		return
	}

	now := time.Now()
	// Scan from tail (oldest) to head
	var next *list.Element
	for elem := sc.evictList.Back(); elem != nil; elem = next {
		next = elem.Prev()
		entry := elem.Value.(*lruEntry)
		if now.Sub(entry.session.UpdatedAt) > sc.ttl {
			sc.removeElement(elem)
		}
	}
}
