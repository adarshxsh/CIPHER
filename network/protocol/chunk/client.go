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

type Client struct {
	mu               sync.Mutex
	transport        *transport.Transport
	peerID           peer.ID
	stream           network.Stream
	engine           *engine.ContentEngine
	digest           core.Digest
	transactionCount int
	closed           bool
	ticketGenerator  TicketGeneratorFunc
}

func (c *Client) SetTicketGenerator(fn TicketGeneratorFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ticketGenerator = fn
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
		if c.transactionCount >= MaxTransactionsPerStream {
			// Check if remote peer sent an error or terminated the stream
			type readResult struct {
				msg *Message
				err error
			}
			ch := make(chan readResult, 1)
			oldStream := c.stream
			go func() {
				msg, err := ReadMessage(oldStream)
				ch <- readResult{msg: msg, err: err}
			}()

			select {
			case res := <-ch:
				if res.err == nil && res.msg != nil && res.msg.Type == MsgError {
					code, errMsg, _ := ParseError(res.msg.Payload)
					c.closed = true
					c.closeStreamInternal()
					return nil, fmt.Errorf("remote error (code %d): %s", code, errMsg)
				}
				if res.err != nil {
					c.closed = true
					c.closeStreamInternal()
					return nil, fmt.Errorf("stream terminated by remote peer: %w", res.err)
				}
				c.closeStreamInternal()
			case <-time.After(5 * time.Millisecond):
				// Stream is open and idle; previous transaction completed without remote error
				c.closeStreamInternal()
			}
		} else {
			return c.stream, nil
		}
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

	// Send Ticket if generator is configured, otherwise send standard ACK
	c.mu.Lock()
	tg := c.ticketGenerator
	c.mu.Unlock()
	if tg != nil {
		signedTicket, err := tg(chunkID)
		if err != nil {
			c.closeOnError()
			log.Printf("[Chunk Protocol] Failed to generate ticket for %x: %v", chunkID, err)
			return nil, fmt.Errorf("failed to generate ticket: %w", err)
		}
		ticketMsg, err := BuildTicket(signedTicket)
		if err != nil {
			c.closeOnError()
			return nil, fmt.Errorf("failed to build ticket message: %w", err)
		}
		if err := WriteMessage(s, ticketMsg); err != nil {
			c.closeOnError()
			log.Printf("[Chunk Protocol] Failed to send ticket for %x: %v", chunkID, err)
			return nil, fmt.Errorf("failed to send ticket message: %w", err)
		}
	} else {
		// Send ACK (optional fire-and-forget)
		ack := BuildAck(chunkID, 0)
		if err := WriteMessage(s, ack); err != nil {
			log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
		}
	}

	c.recordTransactionAndCheck()
	return chunk, nil
}
