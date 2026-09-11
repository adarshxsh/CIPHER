package chunk

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/transport"
)

type Client struct {
	transport  *transport.Transport
	peerID     peer.ID
	engine     *engine.ContentEngine
	digest     core.Digest
	ctx        context.Context
	cancel     context.CancelFunc
	closed     bool
	mu         sync.Mutex
	activeStrs map[network.Stream]struct{}
}

// NewClient creates a new chunk client that communicates with a remote peer over short-lived per-transaction streams.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	clientCtx, cancel := context.WithCancel(ctx)
	return &Client{
		transport:  t,
		peerID:     peerID,
		engine:     eng,
		digest:     verifier.NewSHA256Digest(),
		ctx:        clientCtx,
		cancel:     cancel,
		activeStrs: make(map[network.Stream]struct{}),
	}, nil
}

func (c *Client) openStream(ctx context.Context) (network.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("client is closed")
	}
	c.mu.Unlock()

	select {
	case <-c.ctx.Done():
		return nil, fmt.Errorf("client context cancelled: %w", c.ctx.Err())
	default:
	}

	stream, err := c.transport.OpenStream(ctx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		stream.Reset()
		return nil, fmt.Errorf("client is closed")
	}
	c.activeStrs[stream] = struct{}{}
	c.mu.Unlock()

	return stream, nil
}

func (c *Client) releaseStream(stream network.Stream) {
	c.mu.Lock()
	delete(c.activeStrs, stream)
	c.mu.Unlock()
	stream.Close()
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	for s := range c.activeStrs {
		s.Reset()
	}
	c.activeStrs = nil
	c.mu.Unlock()
	return nil
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	stream, err := c.openStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream for REQUEST_MANIFEST: %w", err)
	}
	defer c.releaseStream(stream)

	req := BuildRequestManifest(id)
	if err := WriteMessage(stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
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
	stream, err := c.openStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream for REQUEST_CHUNK: %w", err)
	}
	defer c.releaseStream(stream)

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
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
		WriteMessage(stream, errMsg)
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send ACK
	ack := BuildAck(chunkID, 0)
	if err := WriteMessage(stream, ack); err != nil {
		log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
	}

	return chunk, nil
}
