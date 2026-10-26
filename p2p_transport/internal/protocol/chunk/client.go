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

var ErrRemoteChunkNotFound = fmt.Errorf("remote error: chunk not found")

type Client struct {
	transport *transport.Transport
	peerID    peer.ID
	engine    *engine.ContentEngine
	digest    core.Digest

	mu     sync.Mutex
	stream network.Stream
	closed bool
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	stream, err := t.OpenStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		transport: t,
		peerID:    peerID,
		stream:    stream,
		engine:    eng,
		digest:    verifier.NewSHA256Digest(),
	}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	s := c.stream
	c.stream = nil
	c.mu.Unlock()

	if s != nil {
		return s.Close()
	}
	return nil
}

func (c *Client) resetStream() {
	c.mu.Lock()
	s := c.stream
	c.stream = nil
	c.mu.Unlock()

	if s != nil {
		_ = s.Close()
	}
}

func (c *Client) ensureStream(ctx context.Context) (network.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("client is closed")
	}
	if c.stream != nil {
		s := c.stream
		c.mu.Unlock()
		return s, nil
	}
	c.mu.Unlock()

	stream, err := c.transport.OpenStream(ctx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = stream.Close()
		return nil, fmt.Errorf("client is closed")
	}
	c.stream = stream
	return stream, nil
}

func (c *Client) sendAndReceive(ctx context.Context, req *Message) (*Message, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		stream, err := c.ensureStream(ctx)
		if err != nil {
			return nil, err
		}

		if err := WriteMessage(stream, req); err != nil {
			c.resetStream()
			lastErr = err
			continue
		}

		resp, err := ReadMessage(stream)
		if err != nil {
			c.resetStream()
			lastErr = err
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	req := BuildRequestManifest(id)
	resp, err := c.sendAndReceive(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send/receive REQUEST_MANIFEST: %w", err)
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
	req := BuildRequestChunk(chunkID)
	resp, err := c.sendAndReceive(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send/receive REQUEST_CHUNK: %w", err)
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
	c.mu.Lock()
	stream := c.stream
	c.mu.Unlock()

	hash := c.digest.Sum(chunk.Data)
	if hash != core.Hash(chunkID) {
		if stream != nil {
			errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
			_ = WriteMessage(stream, errMsg)
		}
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send ACK (optional fire-and-forget)
	ack := BuildAck(chunkID, 0)
	if stream != nil {
		if err := WriteMessage(stream, ack); err != nil {
			log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
		}
	}

	return chunk, nil
}
