package chunk

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/network/content/core"
	"cipher/network/content/engine"
	"cipher/network/content/verifier"
	"cipher/network/payments"
	"cipher/network/protocol"
	"cipher/network/transport"
)

var ErrRemoteChunkNotFound = fmt.Errorf("remote error: chunk not found")

// TicketGeneratorFunc creates and signs a RoundTicket for a received and verified chunk.
type TicketGeneratorFunc func(chunkID core.ChunkID) (*payments.SignedTicket, error)

// Client talks to one remote provider. The provider serves a single
// transaction per stream, so every Resolve/FetchChunk runs on its own stream.
type Client struct {
	transport       *transport.Transport
	peerID          peer.ID
	engine          *engine.ContentEngine
	digest          core.Digest
	ticketGenerator TicketGeneratorFunc

	mu      sync.Mutex
	closed  bool
	pending network.Stream // opened by NewClient, used by the first request
	active  network.Stream // stream of the request in flight, if any
}

var ErrClientClosed = errors.New("chunk client closed")

func (c *Client) SetTicketGenerator(fn TicketGeneratorFunc) {
	c.ticketGenerator = fn
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
// It opens the first stream eagerly so an unreachable peer is reported here, as before.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	stream, err := t.OpenStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		transport: t,
		peerID:    peerID,
		engine:    eng,
		digest:    verifier.NewSHA256Digest(),
		pending:   stream,
	}, nil
}

// Close stops the client. A request in flight is aborted and later requests
// fail with ErrClientClosed.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var err error
	if c.pending != nil {
		err = c.pending.Close()
		c.pending = nil
	}
	if c.active != nil {
		c.active.Reset()
	}
	return err
}

// openTxStream returns the stream for one transaction, with a deadline of
// StreamTimeout or the context deadline, whichever comes first.
func (c *Client) openTxStream(ctx context.Context) (network.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClientClosed
	}
	s := c.pending
	c.pending = nil
	c.mu.Unlock()

	if s == nil {
		var err error
		s, err = c.transport.OpenStream(ctx, c.peerID, protocol.ChunkTransportProtocolID)
		if err != nil {
			return nil, err
		}
	}

	deadline := time.Now().Add(StreamTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := s.SetDeadline(deadline); err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to set stream deadline: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		s.Reset()
		return nil, ErrClientClosed
	}
	c.active = s
	return s, nil
}

func (c *Client) finishTx(s network.Stream) {
	c.mu.Lock()
	if c.active == s {
		c.active = nil
	}
	c.mu.Unlock()
	s.Close()
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	stream, err := c.openTxStream(ctx)
	if err != nil {
		return nil, err
	}
	defer c.finishTx(stream)

	req := BuildRequestManifest(id)
	if err := WriteMessage(stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadValidatedMessage(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
		return nil, fmt.Errorf("remote error (code %d): %q", code, msg)
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
	stream, err := c.openTxStream(ctx)
	if err != nil {
		return nil, err
	}
	defer c.finishTx(stream)

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadValidatedMessage(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
		if code == ErrChunkNotFound {
			return nil, ErrRemoteChunkNotFound
		}
		return nil, fmt.Errorf("remote error (code %d): %q", code, msg)
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

	// Send Ticket if generator is configured, otherwise send standard ACK
	if c.ticketGenerator != nil {
		signedTicket, err := c.ticketGenerator(chunkID)
		if err != nil {
			log.Printf("[Chunk Protocol] Failed to generate ticket for %x: %v", chunkID, err)
			return nil, fmt.Errorf("failed to generate ticket: %w", err)
		}
		ticketMsg, err := BuildTicket(signedTicket)
		if err != nil {
			return nil, fmt.Errorf("failed to build ticket message: %w", err)
		}
		if err := WriteMessage(stream, ticketMsg); err != nil {
			log.Printf("[Chunk Protocol] Failed to send ticket for %x: %v", chunkID, err)
			return nil, fmt.Errorf("failed to send ticket message: %w", err)
		}
	} else {
		// Send ACK (optional fire-and-forget)
		ack := BuildAck(chunkID, 0)
		if err := WriteMessage(stream, ack); err != nil {
			log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
		}
	}

	return chunk, nil
}
