package chunk

import (
	"context"
	"errors"
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
	mu               sync.Mutex
	transport        *transport.Transport
	peerID           peer.ID
	stream           network.Stream
	engine           *engine.ContentEngine
	digest           core.Digest
	transactionCount int
	closed           bool
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	c := &Client{
		transport: t,
		peerID:    peerID,
		engine:    eng,
		digest:    verifier.NewSHA256Digest(),
	}
	if _, err := c.ensureStream(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) ensureStream(ctx context.Context) (network.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("client closed")
	}

	if c.stream != nil {
		return c.stream, nil
	}

	if c.transport == nil {
		return nil, errors.New("no transport configured")
	}

	s, err := c.transport.OpenStream(ctx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	c.stream = s
	c.transactionCount = 0
	return s, nil
}

func (c *Client) closeStreamInternal() {
	if c.stream != nil {
		_ = c.stream.Close()
		c.stream = nil
	}
	c.transactionCount = 0
}

func (c *Client) closeOnError() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeStreamInternal()
}

func (c *Client) recordTransactionAndCheck() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.transactionCount++
	if c.transactionCount >= MaxTransactionsPerStream {
		c.closeStreamInternal()
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	if c.stream != nil {
		err := c.stream.Close()
		c.stream = nil
		c.transactionCount = 0
		return err
	}
	return nil
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	s, err := c.ensureStream(ctx)
	if err != nil {
		return nil, err
	}

	req := BuildRequestManifest(id)
	if err := WriteMessage(s, req); err != nil {
		c.closeOnError()
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadMessage(s)
	if err != nil {
		c.closeOnError()
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		c.closeOnError()
		code, msg, _ := ParseError(resp.Payload)
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgManifest {
		c.closeOnError()
		return nil, fmt.Errorf("expected MANIFEST, got %d", resp.Type)
	}

	respID, data, err := ParseManifest(resp.Payload)
	if err != nil {
		c.closeOnError()
		return nil, err
	}
	if respID != id {
		c.closeOnError()
		return nil, fmt.Errorf("content ID mismatch in response")
	}

	c.recordTransactionAndCheck()
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
	s, err := c.ensureStream(ctx)
	if err != nil {
		return nil, err
	}

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(s, req); err != nil {
		c.closeOnError()
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadMessage(s)
	if err != nil {
		c.closeOnError()
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		c.closeOnError()
		code, msg, _ := ParseError(resp.Payload)
		if code == ErrChunkNotFound {
			return nil, ErrRemoteChunkNotFound
		}
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgChunk {
		c.closeOnError()
		return nil, fmt.Errorf("expected CHUNK, got %d", resp.Type)
	}

	chunk, err := ParseChunk(resp.Payload)
	if err != nil {
		c.closeOnError()
		return nil, fmt.Errorf("failed to parse chunk: %w", err)
	}

	// Verify Hash matches ChunkID
	hash := c.digest.Sum(chunk.Data)
	if hash != core.Hash(chunkID) {
		errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
		_ = WriteMessage(s, errMsg)
		c.closeOnError()
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send ACK
	ack := BuildAck(chunkID, 0)
	if err := WriteMessage(s, ack); err != nil {
		log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
	}

	c.recordTransactionAndCheck()
	return chunk, nil
}
