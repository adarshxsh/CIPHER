package reputation

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// FailureType represents the category of failure encountered with a peer.
type FailureType int

const (
	FailureGeneric FailureType = iota
	FailureCorruptData
	FailureTimeout
	FailureNetworkError
	FailureBadRequest
)

// Config defines parameters for peer failure scoring and resource bounds.
type Config struct {
	// BanThreshold is the failure score at or above which a peer is banned.
	BanThreshold float64
	// InitialScore is the starting failure score for new peers (default 0).
	InitialScore float64
	// PenaltyCorruptData is added on data corruption/integrity failure.
	PenaltyCorruptData float64
	// PenaltyTimeout is added on request timeout or context deadline.
	PenaltyTimeout float64
	// PenaltyNetworkError is added on stream or connection errors.
	PenaltyNetworkError float64
	// PenaltyBadRequest is added on malformed wire messages.
	PenaltyBadRequest float64
	// PenaltyGeneric is added on generic/unclassified errors.
	PenaltyGeneric float64
	// SuccessReward is subtracted from failure score on successful chunk download.
	SuccessReward float64
	// HalfLife is the duration over which a peer's failure score decays by half towards 0.
	HalfLife time.Duration
	// MaxTrackedPeers is the maximum number of peer records held in memory.
	MaxTrackedPeers int
}

// DefaultConfig returns recommended default settings for PeerScoreManager.
func DefaultConfig() Config {
	return Config{
		BanThreshold:        100.0,
		InitialScore:        0.0,
		PenaltyCorruptData:  50.0,
		PenaltyTimeout:      25.0,
		PenaltyNetworkError: 15.0,
		PenaltyBadRequest:   40.0,
		PenaltyGeneric:      20.0,
		SuccessReward:       10.0,
		HalfLife:            5 * time.Minute,
		MaxTrackedPeers:     10000,
	}
}

type peerEntry struct {
	peerID      peer.ID
	score       float64
	lastUpdated time.Time
	element     *list.Element
}

// ScoreManager manages peer failure scores and reputation in a thread-safe, resource-bounded manner.
type ScoreManager struct {
	mu          sync.RWMutex
	config      Config
	peers       map[peer.ID]*peerEntry
	lruList     *list.List
	bannedPeers map[peer.ID]time.Time
}

// NewScoreManager creates a new ScoreManager with the provided configuration.
func NewScoreManager(cfg Config) *ScoreManager {
	if cfg.BanThreshold <= 0 {
		cfg.BanThreshold = 100.0
	}
	if cfg.MaxTrackedPeers <= 0 {
		cfg.MaxTrackedPeers = 10000
	}
	if cfg.HalfLife <= 0 {
		cfg.HalfLife = 5 * time.Minute
	}

	return &ScoreManager{
		config:      cfg,
		peers:       make(map[peer.ID]*peerEntry),
		lruList:     list.New(),
		bannedPeers: make(map[peer.ID]time.Time),
	}
}

// ClassifyError converts a Go error into a FailureType based on error inspection.
func ClassifyError(err error) FailureType {
	if err == nil {
		return FailureGeneric
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "corrupt") || strings.Contains(msg, "checksum") || strings.Contains(msg, "hash mismatch") || strings.Contains(msg, "verification"):
		return FailureCorruptData
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline") || errors.Is(err, context.DeadlineExceeded):
		return FailureTimeout
	case strings.Contains(msg, "bad request") || strings.Contains(msg, "malformed") || strings.Contains(msg, "protocol"):
		return FailureBadRequest
	case strings.Contains(msg, "stream") || strings.Contains(msg, "connection") || strings.Contains(msg, "reset") || strings.Contains(msg, "closed") || errors.Is(err, context.Canceled):
		return FailureNetworkError
	default:
		return FailureGeneric
	}
}

func (sm *ScoreManager) applyDecay(entry *peerEntry, now time.Time) {
	if sm.config.HalfLife <= 0 || entry.score <= 0 {
		entry.lastUpdated = now
		return
	}
	elapsed := now.Sub(entry.lastUpdated)
	if elapsed <= 0 {
		return
	}
	decayFactor := math.Pow(0.5, float64(elapsed)/float64(sm.config.HalfLife))
	entry.score *= decayFactor
	if entry.score < 0.001 {
		entry.score = 0
	}
	entry.lastUpdated = now
}

// RecordFailure records a failure for the specified peer based on failure type or error.
func (sm *ScoreManager) RecordFailure(ctx context.Context, p peer.ID, failureType FailureType) (float64, bool) {
	if ctx != nil && ctx.Err() != nil {
		return 0, false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	penalty := sm.getPenaltyAmount(failureType)

	entry, exists := sm.peers[p]
	if !exists {
		sm.evictIfFullLocked()
		entry = &peerEntry{
			peerID:      p,
			score:       sm.config.InitialScore,
			lastUpdated: now,
		}
		entry.element = sm.lruList.PushFront(entry)
		sm.peers[p] = entry
	} else {
		sm.lruList.MoveToFront(entry.element)
		sm.applyDecay(entry, now)
	}

	entry.score += penalty
	banned := entry.score >= sm.config.BanThreshold
	if banned {
		sm.bannedPeers[p] = now
	}

	return entry.score, banned
}

// RecordError classifies the error and records the failure for the peer.
func (sm *ScoreManager) RecordError(ctx context.Context, p peer.ID, err error) (float64, bool) {
	ft := ClassifyError(err)
	return sm.RecordFailure(ctx, p, ft)
}

// RecordSuccess records a successful operation for the specified peer, reducing its failure score.
func (sm *ScoreManager) RecordSuccess(ctx context.Context, p peer.ID) float64 {
	if ctx != nil && ctx.Err() != nil {
		return 0
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	entry, exists := sm.peers[p]
	if !exists {
		sm.evictIfFullLocked()
		entry = &peerEntry{
			peerID:      p,
			score:       sm.config.InitialScore,
			lastUpdated: now,
		}
		entry.element = sm.lruList.PushFront(entry)
		sm.peers[p] = entry
	} else {
		sm.lruList.MoveToFront(entry.element)
		sm.applyDecay(entry, now)
	}

	entry.score -= sm.config.SuccessReward
	if entry.score < 0 {
		entry.score = 0
	}

	// If score dropped below threshold, remove from bannedPeers
	if entry.score < sm.config.BanThreshold {
		delete(sm.bannedPeers, p)
	}

	return entry.score
}

// IsBanned returns true if the specified peer is currently banned due to exceeding the failure score threshold.
func (sm *ScoreManager) IsBanned(ctx context.Context, p peer.ID) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	entry, exists := sm.peers[p]
	if !exists {
		_, isBanned := sm.bannedPeers[p]
		return isBanned
	}

	now := time.Now()
	// Calculate current score with decay
	score := entry.score
	if sm.config.HalfLife > 0 && score > 0 {
		elapsed := now.Sub(entry.lastUpdated)
		if elapsed > 0 {
			decayFactor := math.Pow(0.5, float64(elapsed)/float64(sm.config.HalfLife))
			score *= decayFactor
		}
	}

	return score >= sm.config.BanThreshold
}

// GetScore returns the current decayed failure score for a peer.
func (sm *ScoreManager) GetScore(ctx context.Context, p peer.ID) float64 {
	if ctx != nil && ctx.Err() != nil {
		return 0
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	entry, exists := sm.peers[p]
	if !exists {
		return 0
	}

	now := time.Now()
	sm.applyDecay(entry, now)
	return entry.score
}

// FilterPeers returns a slice containing only unbanned peers from the provided slice.
func (sm *ScoreManager) FilterPeers(ctx context.Context, peers []peer.ID) []peer.ID {
	if ctx != nil && ctx.Err() != nil {
		return peers
	}

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	now := time.Now()
	unbanned := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		entry, exists := sm.peers[p]
		if !exists {
			if _, isBanned := sm.bannedPeers[p]; !isBanned {
				unbanned = append(unbanned, p)
			}
			continue
		}

		score := entry.score
		if sm.config.HalfLife > 0 && score > 0 {
			elapsed := now.Sub(entry.lastUpdated)
			if elapsed > 0 {
				decayFactor := math.Pow(0.5, float64(elapsed)/float64(sm.config.HalfLife))
				score *= decayFactor
			}
		}

		if score < sm.config.BanThreshold {
			unbanned = append(unbanned, p)
		}
	}
	return unbanned
}

// TrackedCount returns the number of peers currently tracked in memory.
func (sm *ScoreManager) TrackedCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.peers)
}

// Reset clears all tracked peer scores and ban statuses.
func (sm *ScoreManager) Reset() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.peers = make(map[peer.ID]*peerEntry)
	sm.lruList.Init()
	sm.bannedPeers = make(map[peer.ID]time.Time)
}

func (sm *ScoreManager) getPenaltyAmount(ft FailureType) float64 {
	switch ft {
	case FailureCorruptData:
		return sm.config.PenaltyCorruptData
	case FailureTimeout:
		return sm.config.PenaltyTimeout
	case FailureNetworkError:
		return sm.config.PenaltyNetworkError
	case FailureBadRequest:
		return sm.config.PenaltyBadRequest
	default:
		return sm.config.PenaltyGeneric
	}
}

func (sm *ScoreManager) evictIfFullLocked() {
	if len(sm.peers) < sm.config.MaxTrackedPeers {
		return
	}

	// Remove oldest accessed peer from LRU list that is not banned, or simply oldest
	elem := sm.lruList.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*peerEntry)
	sm.lruList.Remove(elem)
	delete(sm.peers, entry.peerID)
}

func (ft FailureType) String() string {
	switch ft {
	case FailureCorruptData:
		return "CorruptData"
	case FailureTimeout:
		return "Timeout"
	case FailureNetworkError:
		return "NetworkError"
	case FailureBadRequest:
		return "BadRequest"
	default:
		return "GenericFailure"
	}
}

func (sm *ScoreManager) FormatDebug(p peer.ID) string {
	score := sm.GetScore(context.Background(), p)
	banned := sm.IsBanned(context.Background(), p)
	return fmt.Sprintf("Peer %s: Score=%.2f, Banned=%t", p, score, banned)
}
