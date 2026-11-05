package reputation

import (
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Config defines configuration parameters for PeerTracker.
type Config struct {
	InitialScore    int
	PenaltyPerError int
	MaxErrors       int
	MinScore        int
	MaxTrackedPeers int
}

// DefaultConfig returns reasonable default configuration parameters.
func DefaultConfig() Config {
	return Config{
		InitialScore:    100,
		PenaltyPerError: 35,
		MaxErrors:       3,
		MinScore:        0,
		MaxTrackedPeers: 10000,
	}
}

// PeerStats represents a snapshot of reputation metrics for a single peer.
type PeerStats struct {
	PeerID       peer.ID   `json:"peer_id"`
	Score        int       `json:"score"`
	ErrorCount   int       `json:"error_count"`
	HashFailures int       `json:"hash_failures"`
	Blacklisted  bool      `json:"blacklisted"`
	LastSeen     time.Time `json:"last_seen"`
}

type peerRecord struct {
	peerID       peer.ID
	score        int
	errorCount   int
	hashFailures int
	blacklisted  bool
	lastSeen     time.Time
}

// PeerTracker manages reputation scores, error metrics, and blacklisting for P2P peers.
type PeerTracker struct {
	mu     sync.RWMutex
	config Config
	peers  map[peer.ID]*peerRecord
}

var (
	defaultTrackerOnce sync.Once
	defaultTracker     *PeerTracker
)

// Default returns a shared global PeerTracker with default configuration.
func Default() *PeerTracker {
	defaultTrackerOnce.Do(func() {
		defaultTracker = NewPeerTracker(DefaultConfig())
	})
	return defaultTracker
}

// NewPeerTracker creates a new PeerTracker instance with the given configuration.
func NewPeerTracker(cfg ...Config) *PeerTracker {
	c := DefaultConfig()
	if len(cfg) > 0 {
		c = cfg[0]
		if c.MaxErrors <= 0 {
			c.MaxErrors = 3
		}
		if c.MaxTrackedPeers <= 0 {
			c.MaxTrackedPeers = 10000
		}
		if c.PenaltyPerError <= 0 {
			c.PenaltyPerError = 35
		}
	}

	return &PeerTracker{
		config: c,
		peers:  make(map[peer.ID]*peerRecord),
	}
}

// getOrCreateLocked gets an existing record or initializes a new record for peerID.
// Must be called with pt.mu write lock held.
func (pt *PeerTracker) getOrCreateLocked(peerID peer.ID) *peerRecord {
	rec, exists := pt.peers[peerID]
	if !exists {
		// Enforce bounded memory footprint
		if len(pt.peers) >= pt.config.MaxTrackedPeers {
			pt.evictOldestLocked()
		}
		rec = &peerRecord{
			peerID:   peerID,
			score:    pt.config.InitialScore,
			lastSeen: time.Now(),
		}
		pt.peers[peerID] = rec
	} else {
		rec.lastSeen = time.Now()
	}
	return rec
}

// evictOldestLocked removes non-blacklisted peers with 0 errors or oldest lastSeen.
func (pt *PeerTracker) evictOldestLocked() {
	var oldestID peer.ID
	var oldestTime time.Time
	found := false

	for id, rec := range pt.peers {
		if rec.blacklisted {
			continue // Do not evict blacklisted peers
		}
		if !found || rec.lastSeen.Before(oldestTime) {
			oldestID = id
			oldestTime = rec.lastSeen
			found = true
		}
	}

	if found {
		delete(pt.peers, oldestID)
	}
}

// RecordHashFailure records a hash verification failure for peerID.
// It penalizes the peer score and increments error counters.
// If the error count reaches MaxErrors or score drops to MinScore, the peer is blacklisted.
// Returns (newScore, isBlacklisted).
func (pt *PeerTracker) RecordHashFailure(peerID peer.ID) (int, bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	rec := pt.getOrCreateLocked(peerID)
	rec.hashFailures++
	rec.errorCount++
	rec.score -= pt.config.PenaltyPerError
	if rec.score < pt.config.MinScore {
		rec.score = pt.config.MinScore
	}

	if rec.hashFailures >= pt.config.MaxErrors || rec.errorCount >= pt.config.MaxErrors || rec.score <= pt.config.MinScore {
		rec.blacklisted = true
	}

	return rec.score, rec.blacklisted
}

// RecordError records a general protocol or processing error for peerID.
// Returns (newScore, isBlacklisted).
func (pt *PeerTracker) RecordError(peerID peer.ID) (int, bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	rec := pt.getOrCreateLocked(peerID)
	rec.errorCount++
	rec.score -= pt.config.PenaltyPerError
	if rec.score < pt.config.MinScore {
		rec.score = pt.config.MinScore
	}

	if rec.errorCount >= pt.config.MaxErrors || rec.score <= pt.config.MinScore {
		rec.blacklisted = true
	}

	return rec.score, rec.blacklisted
}

// IsBlacklisted returns whether peerID is blacklisted.
func (pt *PeerTracker) IsBlacklisted(peerID peer.ID) bool {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	rec, exists := pt.peers[peerID]
	if !exists {
		return false
	}
	return rec.blacklisted
}

// GetScore returns the current score for peerID.
func (pt *PeerTracker) GetScore(peerID peer.ID) int {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	rec, exists := pt.peers[peerID]
	if !exists {
		return pt.config.InitialScore
	}
	return rec.score
}

// GetErrorCount returns total error count for peerID.
func (pt *PeerTracker) GetErrorCount(peerID peer.ID) int {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	rec, exists := pt.peers[peerID]
	if !exists {
		return 0
	}
	return rec.errorCount
}

// GetHashFailures returns hash failure count for peerID.
func (pt *PeerTracker) GetHashFailures(peerID peer.ID) int {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	rec, exists := pt.peers[peerID]
	if !exists {
		return 0
	}
	return rec.hashFailures
}

// Blacklist explicitly blacklists peerID.
func (pt *PeerTracker) Blacklist(peerID peer.ID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	rec := pt.getOrCreateLocked(peerID)
	rec.blacklisted = true
	rec.score = pt.config.MinScore
}

// Unblacklist removes blacklist status and resets reputation for peerID.
func (pt *PeerTracker) Reset(peerID peer.ID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	delete(pt.peers, peerID)
}

// DisconnectAndBlacklist marks peerID as blacklisted and closes all connections to peerID on host h.
func (pt *PeerTracker) DisconnectAndBlacklist(h host.Host, peerID peer.ID) error {
	pt.Blacklist(peerID)
	if h != nil {
		if err := h.Network().ClosePeer(peerID); err != nil {
			return fmt.Errorf("failed to close connections to peer %s: %w", peerID, err)
		}
	}
	return nil
}

// GetPeerStats returns a snapshot of peer metrics for peerID.
func (pt *PeerTracker) GetPeerStats(peerID peer.ID) (PeerStats, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	rec, exists := pt.peers[peerID]
	if !exists {
		return PeerStats{}, false
	}

	return PeerStats{
		PeerID:       rec.peerID,
		Score:        rec.score,
		ErrorCount:   rec.errorCount,
		HashFailures: rec.hashFailures,
		Blacklisted:  rec.blacklisted,
		LastSeen:     rec.lastSeen,
	}, true
}
