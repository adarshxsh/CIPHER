package chunk

import (
	"context"

	"cipher/internal/content/core"
)

// ChunkInterceptor defines an interface for intercepting chunk payload handling.
type ChunkInterceptor interface {
	InterceptChunk(ctx context.Context, chunk *core.Chunk) (*core.Chunk, error)
}
