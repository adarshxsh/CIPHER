package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type Client struct {
	transport *transport.Transport
	peerID    peer.ID
	engine    *engine.ContentEngine
	digest    core.Digest

	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	closed       bool
	activeStream network.Stream

	ticketGenerator TicketGeneratorFunc
}

func (c *Client) SetTicketGenerator(fn TicketGeneratorFunc) {
	c.mu.Lock()
	c.ticketGenerator = fn
	c.mu.Unlock()
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	clientCtx, cancel := context.WithCancel(ctx)
	return &Client{
		transport: t,
		peerID:    peerID,
		engine:    eng,
		digest:    verifier.NewSHA256Digest(),
		ctx:       clientCtx,
		cancel:    cancel,
	}, nil
}

func (c *Client) getOrCreateStream(ctx context.Context) (network.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	if c.activeStream != nil {
		c.activeStream.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
		msg, err := ReadMessage(c.activeStream)
		c.activeStream.SetReadDeadline(time.Time{})

		if err == nil {
			c.activeStream.Close()
			c.activeStream = nil
			if msg.Type == MsgError {
				code, errMsg, _ := ParseError(msg.Payload)
				return nil, fmt.Errorf("remote error (code %d): %s", code, errMsg)
			}
			return nil, fmt.Errorf("unexpected message on stream: %d", msg.Type)
		}

		if err == io.EOF || err.Error() == "stream reset" || errors.Is(err, network.ErrReset) {
			c.activeStream.Close()
			c.activeStream = nil
		} else if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
			return c.activeStream, nil
		} else {
			c.activeStream.Close()
			c.activeStream = nil
		}
	}

	streamCtx := ctx
	if c.ctx != nil {
		var cancel context.CancelFunc
		streamCtx, cancel = context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-c.ctx.Done():
				cancel()
			case <-streamCtx.Done():
			}
		}()
	}

	stream, err := c.transport.OpenStream(streamCtx, c.peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}

	if c.closed {
		stream.Close()
		return nil, errors.New("client is closed")
	}
	c.activeStream = stream
	return stream, nil
}

func (c *Client) resetActiveStream(s network.Stream) {
	c.mu.Lock()
	if c.activeStream == s {
		c.activeStream.Close()
		c.activeStream = nil
	}
	c.mu.Unlock()
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	if c.activeStream != nil {
		c.activeStream.Reset()
		c.activeStream = nil
	}
	return nil
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	stream, err := c.getOrCreateStream(ctx)
	if err != nil {
		return nil, err
	}

	req := BuildRequestManifest(id)
	if err := WriteMessage(stream, req); err != nil {
		c.resetActiveStream(stream)
		freshStream, err2 := c.getOrCreateStream(ctx)
		if err2 != nil {
			return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
		}
		stream = freshStream
		if err := WriteMessage(stream, req); err != nil {
			return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
		}
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		if err == io.EOF || err.Error() == "stream reset" || errors.Is(err, network.ErrReset) {
			c.resetActiveStream(stream)
			freshStream, err2 := c.getOrCreateStream(ctx)
			if err2 == nil {
				stream = freshStream
				if err := WriteMessage(stream, req); err == nil {
					resp, err = ReadMessage(stream)
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}
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
	stream, err := c.getOrCreateStream(ctx)
	if err != nil {
		return nil, err
	}

	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(stream, req); err != nil {
		c.resetActiveStream(stream)
		freshStream, err2 := c.getOrCreateStream(ctx)
		if err2 != nil {
			return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
		}
		stream = freshStream
		if err := WriteMessage(stream, req); err != nil {
			return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
		}
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		if err == io.EOF || err.Error() == "stream reset" || errors.Is(err, network.ErrReset) {
			c.resetActiveStream(stream)
			freshStream, err2 := c.getOrCreateStream(ctx)
			if err2 == nil {
				stream = freshStream
				if err := WriteMessage(stream, req); err == nil {
					resp, err = ReadMessage(stream)
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}
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
		WriteMessage(stream, errMsg)
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
	}

	// Set the expected ChunkID
	chunk.Header.ID = chunkID

	// Send Ticket if generator is configured, otherwise send standard ACK
	c.mu.Lock()
	tg := c.ticketGenerator
	c.mu.Unlock()

	if tg != nil {
		signedTicket, err := tg(chunkID)
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
