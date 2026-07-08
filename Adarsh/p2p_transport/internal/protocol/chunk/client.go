package chunk

import (
	"context"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/transfer/session"
)

type Client struct {
	stream  network.Stream
	engine  *engine.ContentEngine
	digest  core.Digest
	session *session.TransferSession
	sm      session.SessionManager
}

func NewClient(ctx context.Context, h host.Host, peerID peer.ID, eng *engine.ContentEngine, sm session.SessionManager, s *session.TransferSession) (*Client, error) {
	stream, err := h.NewStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		stream:  stream,
		engine:  eng,
		digest:  verifier.NewSHA256Digest(),
		session: s,
		sm:      sm,
	}, nil
}

func (c *Client) Close() error {
	return c.stream.Close()
}

func (c *Client) Resolve(ctx context.Context, id core.ContentID) ([]byte, error) {
	req := BuildRequestManifest(id)
	if err := WriteMessage(c.stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_MANIFEST: %w", err)
	}

	resp, err := ReadMessage(c.stream)
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

func (c *Client) Download(ctx context.Context, chunkIDs []core.ChunkID) error {
	total := len(chunkIDs)
	for i, chunkID := range chunkIDs {
		processed := i + 1
		
		// 1. Skip logic (Resume Support)
		if c.session != nil && i < len(c.session.Completed) && c.session.Completed[i] {
			fmt.Printf("\r\033[K[Progress] %d/%d chunks (%.1f%%) - Skipped", processed, total, float64(processed)/float64(total)*100)
			continue
		}
		has, _ := c.engine.HasChunk(ctx, chunkID)
		if has {
			if c.session != nil && i < len(c.session.Completed) {
				c.session.Completed[i] = true
			}
			if c.sm != nil && c.session != nil {
				c.sm.Save(c.session)
			}
			fmt.Printf("\r\033[K[Progress] %d/%d chunks (%.1f%%) - Skipped", processed, total, float64(processed)/float64(total)*100)
			continue
		}

		// 2. Request & Receive Chunk with Retry
		var chunk *core.Chunk
		err := session.ExecuteWithRetry(ctx, session.DefaultRetryPolicy, func() error {
			req := BuildRequestChunk(chunkID)
			if err := WriteMessage(c.stream, req); err != nil {
				return fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
			}

			resp, err := ReadMessage(c.stream)
			if err != nil {
				return fmt.Errorf("failed to read response: %w", err)
			}

			if resp.Type == MsgError {
				code, msg, _ := ParseError(resp.Payload)
				return fmt.Errorf("remote error (code %d): %s", code, msg)
			}

			if resp.Type != MsgChunk {
				return fmt.Errorf("expected CHUNK, got %d", resp.Type)
			}

			var parseErr error
			chunk, parseErr = ParseChunk(resp.Payload)
			if parseErr != nil {
				return fmt.Errorf("failed to parse chunk: %w", parseErr)
			}
			return nil
		})
		
		if err != nil {
			return fmt.Errorf("failed to fetch chunk %d after retries: %w", i, err)
		}

		// 3. Verify Hash matches ChunkID
		hash := c.digest.Sum(chunk.Data)
		if hash != core.Hash(chunkID) {
			errMsg := BuildError(ErrIntegrityMismatch, "chunk hash mismatch")
			WriteMessage(c.stream, errMsg)
			return fmt.Errorf("corrupted chunk %x received", chunkID)
		}
		
		// Set the expected ChunkID
		chunk.Header.ID = chunkID

		// 4. Store Chunk
		if err := c.engine.PutChunk(ctx, chunk); err != nil {
			errMsg := BuildError(ErrInternal, "failed to store chunk locally")
			WriteMessage(c.stream, errMsg)
			return fmt.Errorf("failed to store chunk %x: %w", chunkID, err)
		}

		// 5. Send ACK
		ack := BuildAck(chunkID, 0)
		if err := WriteMessage(c.stream, ack); err != nil {
			log.Printf("[Chunk Protocol] Failed to send ACK for %x: %v", chunkID, err)
		}

		// 6. Persist Progress
		if c.session != nil && i < len(c.session.Completed) {
			c.session.Completed[i] = true
		}
		if c.sm != nil && c.session != nil {
			c.sm.Save(c.session)
		}
		
		fmt.Printf("\r\033[K[Progress] %d/%d chunks (%.1f%%)", processed, total, float64(processed)/float64(total)*100)
	}

	if total > 0 {
		fmt.Println() // Newline after progress bar completes
	}

	return nil
}
