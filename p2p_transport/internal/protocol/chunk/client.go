package chunk

import (
	"context"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/reputation"
	"cipher/internal/transport"
)

var (
	ErrRemoteChunkNotFound = fmt.Errorf("remote error: chunk not found")
	ErrPeerQuarantined     = fmt.Errorf("peer is quarantined")
)

type ClientOption func(*Client)

func WithTracker(tracker *reputation.Tracker) ClientOption {
	return func(c *Client) {
		c.tracker = tracker
	}
}

type Client struct {
	stream  network.Stream
	engine  *engine.ContentEngine
	digest  core.Digest
	peerID  peer.ID
	tracker *reputation.Tracker
}

func (c *Client) SetReputationTracker(tracker *reputation.Tracker) {
	c.tracker = tracker
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine, opts ...ClientOption) (*Client, error) {
	stream, err := t.OpenStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	c := &Client{
		stream: stream,
		engine: eng,
		digest: verifier.NewSHA256Digest(),
		peerID: peerID,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (c *Client) Close() error {
	return c.stream.Close()
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	if c.tracker != nil && c.tracker.IsQuarantined(c.peerID) {
		c.Close()
		return nil, ErrPeerQuarantined
	}

	req := BuildRequestManifest(id)
	if err := WriteMessage(c.stream, req); err != nil {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultTransport)
		}
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadMessage(c.stream)
	if err != nil {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultTransport)
		}
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
		if code != ErrChunkNotFound {
			if c.tracker != nil {
				c.tracker.RecordFault(c.peerID, reputation.FaultProtocol)
			}
		}
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgManifest {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultProtocol)
		}
		return nil, fmt.Errorf("expected MANIFEST, got %d", resp.Type)
	}

	respID, data, err := ParseManifest(resp.Payload)
	if err != nil {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultProtocol)
		}
		return nil, err
	}
	if respID != id {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultProtocol)
		}
		return nil, fmt.Errorf("content ID mismatch in response")
	}

	if c.tracker != nil {
		c.tracker.RecordSuccess(c.peerID)
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
	if c.tracker != nil && c.tracker.IsQuarantined(c.peerID) {
		c.Close()
		return nil, ErrPeerQuarantined
	}

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(c.stream, req); err != nil {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultTransport)
		}
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadMessage(c.stream)
	if err != nil {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultTransport)
		}
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
		if code == ErrChunkNotFound {
			return nil, ErrRemoteChunkNotFound
		}
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultProtocol)
		}
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgChunk {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultProtocol)
		}
		return nil, fmt.Errorf("expected CHUNK, got %d", resp.Type)
	}

	chunk, err := ParseChunk(resp.Payload)
	if err != nil {
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultProtocol)
		}
		return nil, fmt.Errorf("failed to parse chunk: %w", err)
	}

	// Verify Hash matches ChunkID
	hash := c.digest.Sum(chunk.Data)
	if hash != core.Hash(chunkID) {
		errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
		WriteMessage(c.stream, errMsg)
		if c.tracker != nil {
			c.tracker.RecordFault(c.peerID, reputation.FaultHashMismatch)
		}
		c.Close()
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	if c.tracker != nil {
		c.tracker.RecordSuccess(c.peerID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send ACK (optional fire-and-forget)
	ack := BuildAck(chunkID, 0)
	if err := WriteMessage(c.stream, ack); err != nil {
		log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
	}

	return chunk, nil
}
