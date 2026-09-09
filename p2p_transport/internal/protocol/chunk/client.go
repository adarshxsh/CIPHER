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
	"cipher/internal/transfer/reputation"
	"cipher/internal/transport"
)

var ErrRemoteChunkNotFound = fmt.Errorf("remote error: chunk not found")

type Client struct {
	mu         sync.Mutex
	stream     network.Stream
	engine     *engine.ContentEngine
	digest     core.Digest
	peerID     peer.ID
	transport  *transport.Transport
	reputation *reputation.ReputationManager
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine, rep ...*reputation.ReputationManager) (*Client, error) {
	var r *reputation.ReputationManager
	if len(rep) > 0 {
		r = rep[0]
	}

	stream, err := t.OpenStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		if r != nil {
			r.RecordTransientFailure(peerID)
		}
		return nil, err
	}
	return &Client{
		stream:     stream,
		engine:     eng,
		digest:     verifier.NewSHA256Digest(),
		peerID:     peerID,
		transport:  t,
		reputation: r,
	}, nil
}

// SetReputationManager attaches a ReputationManager to the client.
func (c *Client) SetReputationManager(rep *reputation.ReputationManager) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reputation = rep
}

// PeerID returns the remote peer ID associated with this client.
func (c *Client) PeerID() peer.ID {
	return c.peerID
}

func (c *Client) getOrOpenStream(ctx context.Context) (network.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stream != nil {
		return c.stream, nil
	}
	if c.transport == nil {
		return nil, fmt.Errorf("stream is closed and transport is nil")
	}
	s, err := c.transport.OpenStream(ctx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		if c.reputation != nil {
			c.reputation.RecordTransientFailure(c.peerID)
		}
		return nil, fmt.Errorf("failed to open stream to peer %s: %w", c.peerID, err)
	}
	c.stream = s
	return c.stream, nil
}

func (c *Client) resetStream() {
	c.mu.Lock()
	s := c.stream
	c.stream = nil
	c.mu.Unlock()

	if s != nil {
		_ = s.Reset()
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	s := c.stream
	c.stream = nil
	c.mu.Unlock()

	if s != nil {
		return s.Close()
	}
	return nil
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	s, err := c.getOrOpenStream(ctx)
	if err != nil {
		return nil, err
	}

	req := BuildRequestManifest(id)
	if err := WriteMessage(s, req); err != nil {
		c.resetStream()
		if c.reputation != nil {
			c.reputation.RecordTransientFailure(c.peerID)
		}
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadMessage(s)
	if err != nil {
		c.resetStream()
		if c.reputation != nil {
			c.reputation.RecordTransientFailure(c.peerID)
		}
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
	s, err := c.getOrOpenStream(ctx)
	if err != nil {
		return nil, err
	}

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(s, req); err != nil {
		c.resetStream()
		if c.reputation != nil {
			c.reputation.RecordTransientFailure(c.peerID)
		}
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadMessage(s)
	if err != nil {
		c.resetStream()
		if c.reputation != nil {
			c.reputation.RecordTransientFailure(c.peerID)
		}
		return nil, fmt.Errorf("failed to read response: %w", err)
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
		if c.reputation != nil {
			c.reputation.RecordIntegrityFailure(c.peerID)
		}
		errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
		_ = WriteMessage(s, errMsg)
		c.resetStream()
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send ACK (optional fire-and-forget)
	ack := BuildAck(chunkID, 0)
	if err := WriteMessage(s, ack); err != nil {
		log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
	}

	if c.reputation != nil {
		c.reputation.RecordSuccess(c.peerID)
	}

	return chunk, nil
}
