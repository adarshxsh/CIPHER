package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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

type Client struct {
	transport *transport.Transport
	peerID    peer.ID
	engine    *engine.ContentEngine
	digest    core.Digest

	ticketGenerator TicketGeneratorFunc

	mu           sync.Mutex
	activeStream network.Stream
	closed       bool
}

func (c *Client) SetTicketGenerator(fn TicketGeneratorFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ticketGenerator = fn
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	if t == nil {
		return nil, fmt.Errorf("nil transport")
	}
	return &Client{
		transport: t,
		peerID:    peerID,
		engine:    eng,
		digest:    verifier.NewSHA256Digest(),
	}, nil
}

func (c *Client) openStream(ctx context.Context) (network.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	if c.activeStream != nil {
		// Inspect previous stream for closure or server error (e.g. ticket rejection)
		_ = c.activeStream.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		msg, err := ReadMessage(c.activeStream)
		_ = c.activeStream.SetReadDeadline(time.Time{})

		if err == nil {
			if msg.Type == MsgError {
				code, msgStr, _ := ParseError(msg.Payload)
				_ = c.activeStream.Close()
				c.activeStream = nil
				c.closed = true
				return nil, fmt.Errorf("remote error (code %d): %s", code, msgStr)
			}
			_ = c.activeStream.Close()
			c.activeStream = nil
		} else if errors.Is(err, io.EOF) || err == io.EOF {
			// Normal clean closure by server after transaction limit
			_ = c.activeStream.Close()
			c.activeStream = nil
		} else {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// Stream is still open and idle; reuse it
				return c.activeStream, nil
			}
			_ = c.activeStream.Close()
			c.activeStream = nil
		}
	}

	stream, err := c.transport.OpenStream(ctx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	c.activeStream = stream
	return stream, nil
}

func (c *Client) closeStream(stream network.Stream) {
	c.mu.Lock()
	if c.activeStream == stream {
		c.activeStream = nil
	}
	c.mu.Unlock()
	_ = stream.Close()
}

func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	stream := c.activeStream
	c.activeStream = nil
	c.mu.Unlock()

	if stream != nil {
		return stream.Close()
	}
	return nil
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	stream, err := c.openStream(ctx)
	if err != nil {
		return nil, err
	}

	req := BuildRequestManifest(id)
	if err := WriteMessage(stream, req); err != nil {
		c.closeStream(stream)
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		c.closeStream(stream)
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		c.closeStream(stream)
		code, msg, _ := ParseError(resp.Payload)
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgManifest {
		c.closeStream(stream)
		return nil, fmt.Errorf("expected MANIFEST, got %d", resp.Type)
	}

	respID, data, err := ParseManifest(resp.Payload)
	if err != nil {
		c.closeStream(stream)
		return nil, err
	}
	if respID != id {
		c.closeStream(stream)
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
		return nil, err
	}

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(stream, req); err != nil {
		c.closeStream(stream)
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		c.closeStream(stream)
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		c.closeStream(stream)
		code, msg, _ := ParseError(resp.Payload)
		if code == ErrChunkNotFound {
			return nil, ErrRemoteChunkNotFound
		}
		return nil, fmt.Errorf("remote error (code %d): %s", code, msg)
	}

	if resp.Type != MsgChunk {
		c.closeStream(stream)
		return nil, fmt.Errorf("expected CHUNK, got %d", resp.Type)
	}

	chunk, err := ParseChunk(resp.Payload)
	if err != nil {
		c.closeStream(stream)
		return nil, fmt.Errorf("failed to parse chunk: %w", err)
	}

	// Verify Hash matches ChunkID
	hash := c.digest.Sum(chunk.Data)
	if hash != core.Hash(chunkID) {
		errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
		WriteMessage(stream, errMsg)
		c.closeStream(stream)
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send Ticket if generator is configured, otherwise send standard ACK
	c.mu.Lock()
	ticketGen := c.ticketGenerator
	c.mu.Unlock()

	if ticketGen != nil {
		signedTicket, err := ticketGen(chunkID)
		if err != nil {
			c.closeStream(stream)
			log.Printf("[Chunk Protocol] Failed to generate ticket for %x: %v", chunkID, err)
			return nil, fmt.Errorf("failed to generate ticket: %w", err)
		}
		ticketMsg, err := BuildTicket(signedTicket)
		if err != nil {
			c.closeStream(stream)
			return nil, fmt.Errorf("failed to build ticket message: %w", err)
		}
		if err := WriteMessage(stream, ticketMsg); err != nil {
			c.closeStream(stream)
			log.Printf("[Chunk Protocol] Failed to send ticket for %x: %v", chunkID, err)
			return nil, fmt.Errorf("failed to send ticket message: %w", err)
		}
	} else {
		// Send ACK (optional fire-and-forget)
		ack := BuildAck(chunkID, 0)
		if err := WriteMessage(stream, ack); err != nil {
			c.closeStream(stream)
			log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
		}
	}

	return chunk, nil
}
