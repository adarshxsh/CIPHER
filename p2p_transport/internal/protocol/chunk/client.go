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
	transport     *transport.Transport
	peerID        peer.ID
	engine        *engine.ContentEngine
	digest        core.Digest
	mu            sync.Mutex
	initialStream network.Stream
	activeStream  network.Stream
	closed        bool
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	stream, err := t.OpenStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		transport:     t,
		peerID:        peerID,
		engine:        eng,
		digest:        verifier.NewSHA256Digest(),
		initialStream: stream,
	}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	var lastErr error
	if c.initialStream != nil {
		if err := c.initialStream.Close(); err != nil {
			lastErr = err
		}
		c.initialStream = nil
	}
	if c.activeStream != nil {
		if err := c.activeStream.Reset(); err != nil {
			lastErr = err
		}
		c.activeStream = nil
	}
	return lastErr
}

func (c *Client) executeTransaction(ctx context.Context, fn func(s network.Stream) error) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("client closed")
	}

	var s network.Stream
	var err error

	if c.initialStream != nil {
		s = c.initialStream
		c.initialStream = nil
	} else {
		s, err = c.transport.OpenStream(ctx, c.peerID, protocol.ChunkTransportProtocolID)
		if err != nil {
			c.mu.Unlock()
			return err
		}
	}

	c.activeStream = s
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		if c.activeStream == s {
			c.activeStream = nil
		}
		c.mu.Unlock()
		s.Close()
	}()

	return fn(s)
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	var data []byte
	err := c.executeTransaction(ctx, func(s network.Stream) error {
		req := BuildRequestManifest(id)
		if err := WriteMessage(s, req); err != nil {
			return fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
		}

		resp, err := ReadMessage(s)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		if resp.Type == MsgError {
			code, msg, _ := ParseError(resp.Payload)
			return fmt.Errorf("remote error (code %d): %s", code, msg)
		}

		if resp.Type != MsgManifest {
			return fmt.Errorf("expected MANIFEST, got %d", resp.Type)
		}

		respID, manifestBytes, err := ParseManifest(resp.Payload)
		if err != nil {
			return err
		}
		if respID != id {
			return fmt.Errorf("content ID mismatch in response")
		}

		data = manifestBytes
		return nil
	})
	if err != nil {
		return nil, err
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
	var chunk *core.Chunk
	err := c.executeTransaction(ctx, func(s network.Stream) error {
		req := BuildRequestChunk(chunkID)
		if err := WriteMessage(s, req); err != nil {
			return fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
		}

		resp, err := ReadMessage(s)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		if resp.Type == MsgError {
			code, msg, _ := ParseError(resp.Payload)
			if code == ErrChunkNotFound {
				return ErrRemoteChunkNotFound
			}
			return fmt.Errorf("remote error (code %d): %s", code, msg)
		}

		if resp.Type != MsgChunk {
			return fmt.Errorf("expected CHUNK, got %d", resp.Type)
		}

		parsedChunk, err := ParseChunk(resp.Payload)
		if err != nil {
			return fmt.Errorf("failed to parse chunk: %w", err)
		}

		// Verify Hash matches ChunkID
		hash := c.digest.Sum(parsedChunk.Data)
		if hash != core.Hash(chunkID) {
			errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
			WriteMessage(s, errMsg)
			return fmt.Errorf("corrupted chunk %x received", chunkID)
		}

		// Set the expected ChunkID
		parsedChunk.Header.ID = chunkID

		// Send ACK (optional fire-and-forget)
		ack := BuildAck(chunkID, 0)
		if err := WriteMessage(s, ack); err != nil {
			log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
		}

		chunk = parsedChunk
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chunk, nil
}
