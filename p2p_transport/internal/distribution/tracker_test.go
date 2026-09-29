package distribution

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
)

func TestGlobalReplicaTracker(t *testing.T) {
	chunk1 := core.ChunkID{1}
	chunk2 := core.ChunkID{2}
	p1 := peer.ID("peer-1")
	p2 := peer.ID("peer-2")
	p3 := peer.ID("peer-3")

	tracker := NewGlobalReplicaTracker(2)

	// Initially not complete
	chunks := []core.ChunkID{chunk1, chunk2}
	if tracker.IsComplete(chunks) {
		t.Fatalf("expected incomplete on empty tracker")
	}

	// Commit chunk1 to p1
	tracker.SetStatus(chunk1, p1, ReplicaCommitted)
	if tracker.IsChunkSatisfied(chunk1) {
		t.Fatalf("chunk1 should need 2 replicas, has 1")
	}

	// Commit chunk1 to p2 -> chunk1 satisfied
	tracker.SetStatus(chunk1, p2, ReplicaCommitted)
	if !tracker.IsChunkSatisfied(chunk1) {
		t.Fatalf("chunk1 should be satisfied with 2 replicas")
	}

	// But overall not complete because chunk2 has 0 replicas
	if tracker.IsComplete(chunks) {
		t.Fatalf("overall tracker should not be complete without chunk2")
	}

	// Commit chunk2 to p2 and p3
	tracker.SetStatus(chunk2, p2, ReplicaCommitted)
	tracker.SetStatus(chunk2, p3, ReplicaCommitted)

	// Now both satisfied
	if !tracker.IsComplete(chunks) {
		t.Fatalf("tracker should be complete when all chunks have >= 2 replicas")
	}

	satisfied, total := tracker.GetSummary(chunks)
	if satisfied != 2 || total != 2 {
		t.Errorf("summary mismatch: satisfied=%d, total=%d", satisfied, total)
	}
}

func TestTrackerRandomChunks(t *testing.T) {
	chunks := make([]core.ChunkID, 20)
	for i := range chunks {
		_, _ = rand.Read(chunks[i][:])
	}

	tracker := NewGlobalReplicaTracker(2)
	p1 := peer.ID("peer-a")
	p2 := peer.ID("peer-b")

	for _, c := range chunks {
		tracker.SetStatus(c, p1, ReplicaCommitted)
		tracker.SetStatus(c, p2, ReplicaCommitted)
	}

	if !tracker.IsComplete(chunks) {
		t.Errorf("expected all 20 chunks to be complete with 2 replicas")
	}
}

func TestTrackerPreallocation(t *testing.T) {
	chunk1 := core.ChunkID{10}
	chunk2 := core.ChunkID{20}
	chunks := []core.ChunkID{chunk1, chunk2}
	p1 := peer.ID("peer-x")

	// Pre-allocate via constructor slice argument
	tracker := NewGlobalReplicaTracker(2, chunks)
	defer tracker.Release()

	if tracker.GetStatus(chunk1, p1) != ReplicaPending {
		t.Fatalf("expected initial status ReplicaPending for preallocated chunk")
	}

	tracker.SetStatus(chunk1, p1, ReplicaUploading)
	if tracker.GetStatus(chunk1, p1) != ReplicaUploading {
		t.Fatalf("expected status ReplicaUploading")
	}

	// Pre-allocate additional chunks dynamically
	chunk3 := core.ChunkID{30}
	tracker.Preallocate(chunk3)
	tracker.SetStatus(chunk3, p1, ReplicaCommitted)
	if tracker.GetStatus(chunk3, p1) != ReplicaCommitted {
		t.Fatalf("expected status ReplicaCommitted for dynamically preallocated chunk")
	}
}

func TestTrackerSyncPoolMapReuse(t *testing.T) {
	chunk := core.ChunkID{99}
	p1 := peer.ID("peer-reuse-1")
	p2 := peer.ID("peer-reuse-2")

	tracker1 := NewGlobalReplicaTracker(2, chunk)
	tracker1.SetStatus(chunk, p1, ReplicaCommitted)
	tracker1.SetStatus(chunk, p2, ReplicaCommitted)

	if !tracker1.IsChunkSatisfied(chunk) {
		t.Fatalf("expected chunk to be satisfied in tracker1")
	}

	// Release maps back to pool
	tracker1.Release()

	// New tracker acquiring from pool
	tracker2 := NewGlobalReplicaTracker(2)
	tracker2.SetStatus(chunk, p1, ReplicaPending)

	if tracker2.GetStatus(chunk, p1) != ReplicaPending {
		t.Fatalf("expected fresh clean map state from pool, got %v", tracker2.GetStatus(chunk, p1))
	}
	if tracker2.CommittedCount(chunk) != 0 {
		t.Fatalf("expected 0 committed count on new tracker after pool reuse")
	}
	tracker2.Release()
}

func TestTrackerConcurrentAccess(t *testing.T) {
	const numGoroutines = 10
	const numChunks = 50

	chunks := make([]core.ChunkID, numChunks)
	for i := range chunks {
		chunks[i] = core.ChunkID{byte(i + 1)}
	}

	tracker := NewGlobalReplicaTracker(2, chunks)
	defer tracker.Release()

	p1 := peer.ID("peer-conc-1")
	p2 := peer.ID("peer-conc-2")

	done := make(chan struct{})
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			for i, c := range chunks {
				if id%2 == 0 {
					tracker.SetStatus(c, p1, ReplicaCommitted)
				} else {
					tracker.SetStatus(c, p2, ReplicaCommitted)
				}
				_ = tracker.GetStatus(c, p1)
				_ = tracker.CommittedCount(c)
				_ = tracker.IsChunkSatisfied(c)
				if i%10 == 0 {
					_ = tracker.IsComplete(chunks)
				}
			}
			done <- struct{}{}
		}(g)
	}

	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	if !tracker.IsComplete(chunks) {
		t.Fatalf("expected tracker to be complete after concurrent commits")
	}
}

func BenchmarkTrackerPreallocatedReuse(b *testing.B) {
	chunks := make([]core.ChunkID, 100)
	for i := range chunks {
		chunks[i] = core.ChunkID{byte(i)}
	}
	p1 := peer.ID("peer-bm-1")
	p2 := peer.ID("peer-bm-2")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tracker := NewGlobalReplicaTracker(2, chunks)
		for _, c := range chunks {
			tracker.SetStatus(c, p1, ReplicaCommitted)
			tracker.SetStatus(c, p2, ReplicaCommitted)
		}
		tracker.Release()
	}
}
