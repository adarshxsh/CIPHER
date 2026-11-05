package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) {
	tracker := NewPeerTracker(3, 5, 50*time.Millisecond, 2*time.Second)
	sched := &Scheduler{Engine: eng}
	runWorkerLoop(ctx, sched, source, client, queue, tracker, results)
}

func runWorkerLoop(ctx context.Context, sched *Scheduler, source Source, initialClient *chunk.Client, queue *ChunkQueue, tracker *PeerTracker, results chan<- WorkerResult) {
	client := initialClient
	if client != nil && sched != nil && sched.TicketGenerator != nil {
		client.SetTicketGenerator(sched.TicketGenerator)
	}

	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if tracker.IsPenalized(source.PeerID) {
			log.Printf("[Scheduler] Peer %s is penalized, terminating worker loop", source.PeerID)
			return
		}

		if client == nil {
			var err error
			if sched != nil && sched.Transport != nil {
				client, err = chunk.NewClient(ctx, sched.Transport, source.PeerID, sched.Engine)
				if err == nil && sched.TicketGenerator != nil {
					client.SetTicketGenerator(sched.TicketGenerator)
				}
			} else {
				err = fmt.Errorf("no transport available to reconnect to peer %s", source.PeerID)
			}

			if err != nil {
				tracker.RecordFailure(source.PeerID, err)
				if tracker.IsPenalized(source.PeerID) {
					return
				}

				backoff := tracker.GetBackoff(source.PeerID)
				if backoff > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
				}
				continue
			}
		}

		task, ok := queue.NextForPeer(source.PeerID, tracker)
		if !ok {
			return // Queue empty or no suitable task
		}

		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			if errors.Is(err, chunk.ErrRemoteChunkNotFound) {
				if task.MissedPeers == nil {
					task.MissedPeers = make(map[string]bool)
				}
				task.MissedPeers[source.PeerID.String()] = true
				results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}
				continue
			}

			_ = client.Close()
			client = nil

			tracker.RecordFailure(source.PeerID, err)
			if task.FailedPeers == nil {
				task.FailedPeers = make(map[peer.ID]struct{})
			}
			task.FailedPeers[source.PeerID] = struct{}{}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}

			if tracker.IsPenalized(source.PeerID) {
				return
			}

			backoff := tracker.GetBackoff(source.PeerID)
			if backoff > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := sched.Engine.PutChunk(ctx, chunkData); err != nil {
			_ = client.Close()
			client = nil

			tracker.RecordFailure(source.PeerID, err)
			if task.FailedPeers == nil {
				task.FailedPeers = make(map[peer.ID]struct{})
			}
			task.FailedPeers[source.PeerID] = struct{}{}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}

			if tracker.IsPenalized(source.PeerID) {
				return
			}

			backoff := tracker.GetBackoff(source.PeerID)
			if backoff > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
			continue
		}

		tracker.RecordSuccess(source.PeerID)
		results <- WorkerResult{
			Task:             task,
			Error:            nil,
			PeerID:           source.PeerID.String(),
			BytesTransferred: int64(len(chunkData.Data)),
			Duration:         dur,
		}
	}
}
