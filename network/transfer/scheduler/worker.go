package scheduler

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"cipher/network/content/engine"
	"cipher/network/protocol/chunk"
)

// WorkerResult is the result of a worker attempting a chunk
type WorkerResult struct {
	Task             ChunkTask
	Error            error
	PeerID           string // To track contribution
	BytesTransferred int64
	Duration         time.Duration
}

var TestThrottle time.Duration

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, rep *ReputationTracker) {
	peerIDStr := source.PeerID.String()
	for {
		if rep != nil && rep.IsExcluded(peerIDStr) {
			return // Exit worker loop if peer is excluded
		}

		task, ok := queue.Next()
		if !ok {
			return // Queue empty
		}
		
		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[peerIDStr] {
			queue.Push(task)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			if rep != nil {
				recordErrorEvent(rep, peerIDStr, err)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr, Duration: dur}
			if rep != nil && rep.IsExcluded(peerIDStr) {
				return
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			if rep != nil {
				recordErrorEvent(rep, peerIDStr, err)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr, Duration: dur}
			if rep != nil && rep.IsExcluded(peerIDStr) {
				return
			}
			continue
		}

		if rep != nil {
			rep.RecordEvent(peerIDStr, EventSuccess)
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: peerIDStr, BytesTransferred: int64(len(chunkData.Data)), Duration: dur}
	}
}

func recordErrorEvent(rep *ReputationTracker, peerID string, err error) {
	if errors.Is(err, chunk.ErrRemoteChunkNotFound) {
		return // Expected candidate miss, do not penalize reputation
	}
	if isHashMismatch(err) {
		rep.RecordEvent(peerID, EventHashMismatch)
	} else if isTimeout(err) {
		rep.RecordEvent(peerID, EventTimeout)
	} else {
		// Treat other network/transport errors as timeout/failure
		rep.RecordEvent(peerID, EventTimeout)
	}
}

func isHashMismatch(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, chunk.ErrHashMismatch) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "corrupted") || strings.Contains(msg, "hash mismatch") || strings.Contains(msg, "integrity")
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, chunk.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded")
}
