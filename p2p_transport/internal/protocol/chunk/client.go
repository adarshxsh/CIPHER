package chunk

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/transport"
)

var ErrRemoteChunkNotFound = fmt.Errorf("remote error: chunk not found")

type Client struct {
	ctx       context.Context
	cancel    context.CancelFunc
	transport *transport.Transport
	peerID    peer.ID
	engine    *engine.ContentEngine
	digest    core.Digest
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	if t == nil {
		return nil, fmt.Errorf("transport cannot be nil")
	}
	clientCtx, cancel := context.WithCancel(ctx)
	return &Client{
		ctx:       clientCtx,
		cancel:    cancel,
		transport: t,
		peerID:    peerID,
		engine:    eng,
		digest:    verifier.NewSHA256Digest(),
	}, nil
}

func (c *Client) Close() error {
	c.cancel()
	return nil
}

func combineCtx(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	if err := ctx1.Err(); err != nil {
		ctx, cancel := context.WithCancel(ctx1)
		cancel()
		return ctx, cancel
	}
	if err := ctx2.Err(); err != nil {
		ctx, cancel := context.WithCancel(ctx2)
		cancel()
		return ctx, cancel
	}
	ctx, cancel := context.WithCancel(ctx1)
	stop := context.AfterFunc(ctx2, func() {
		cancel()
	})
	return ctx, func() {
		stop()
		cancel()
	}
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	reqCtx, reqCancel := combineCtx(ctx, c.ctx)
	defer reqCancel()

	if err := reqCtx.Err(); err != nil {
		return nil, fmt.Errorf("client closed or context canceled: %w", err)
	}

	stream, err := c.transport.OpenStream(reqCtx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	_ = stream.SetDeadline(time.Now().Add(WriteTimeout + ReadTimeout))

	req := BuildRequestManifest(id)
	if err := WriteMessage(stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if err := ValidateMessage(resp); err != nil {
		return nil, fmt.Errorf("invalid response message: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgManifest {
		return nil, fmt.Errorf("expected MANIFEST, got %d", resp.Type)
	}

	respID, data, err := ParseManifest(resp.Payload)
	if err != nil {
		return nil, err
	}
	if respID != id {
		return nil, fmt.Errorf("content ID mismatch in response")
	}

	return data, nil
}

// Download requests and retrieves a list of chunks from the remote peer, storing them in the local content engine.
func (c *Client) Download(ctx context.Context, chunkIDs []core.ChunkID) error {
	for _, chunkID := range chunkIDs {
		chunk, err := c.FetchChunk(ctx, chunkID)
		if err != nil {
			return err
		}
		if err := c.engine.PutChunk(ctx, chunk); err != nil {
			return fmt.Errorf("failed to store chunk %x: %w", chunkID, err)
		}
	}
	return nil
}

// FetchChunk requests and reads a single chunk from the remote peer, and validates its integrity.
// It DOES NOT store the chunk in the engine, nor does it handle retries or session state.
func (c *Client) FetchChunk(ctx context.Context, chunkID core.ChunkID) (*core.Chunk, error) {
	reqCtx, reqCancel := combineCtx(ctx, c.ctx)
	defer reqCancel()

	if err := reqCtx.Err(); err != nil {
		return nil, fmt.Errorf("client closed or context canceled: %w", err)
	}

	stream, err := c.transport.OpenStream(reqCtx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	_ = stream.SetDeadline(time.Now().Add(WriteTimeout + ReadTimeout + WriteTimeout))

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if err := ValidateMessage(resp); err != nil {
		return nil, fmt.Errorf("invalid response message: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
		if code == ErrChunkNotFound {
			return nil, ErrRemoteChunkNotFound
		}
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgChunk {
		return nil, fmt.Errorf("expected CHUNK, got %d", resp.Type)
	}

	chunk, err := ParseChunk(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse chunk: %w", err)
	}

	// Verify Hash matches ChunkID
	hash := c.digest.Sum(chunk.Data)
	if hash != core.Hash(chunkID) {
		errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
		_ = WriteMessage(stream, errMsg)
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send ACK (optional fire-and-forget)
	ack := BuildAck(chunkID, 0)
	if err := WriteMessage(stream, ack); err != nil {
		log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
	}

	return chunk, nil
}
