package scheduler

import (
	"context"
	"testing"
	"time"
)

func BenchmarkSchedulerQueue_ErrorRequeue(b *testing.B) {
	b.ReportAllocs()
	q := NewChunkQueueWithCapacity(nil, 10000)

	for i := 0; i < 100; i++ {
		_ = q.Push(ChunkTask{Index: i})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task, ok := q.Next()
		if !ok {
			break
		}
		task.Attempts++
		backoff := CalculateBackoffWithBase(task.Attempts, 100*time.Millisecond, 5*time.Second, true)
		task.ReadyAt = time.Now().Add(backoff)
		_ = q.Push(task)
	}
}

func BenchmarkSchedulerQueue_PopForPeerParallel(b *testing.B) {
	b.ReportAllocs()
	q := NewChunkQueueWithCapacity(nil, 10000)

	for i := 0; i < 1000; i++ {
		_ = q.Push(ChunkTask{Index: i})
	}

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			task, ok := q.PopForPeer(ctx, "peer1")
			if ok {
				_ = q.Push(task)
			}
		}
	})
}
