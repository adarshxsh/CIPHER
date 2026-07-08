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
)

type Client struct {
	stream network.Stream
	engine *engine.ContentEngine
	digest core.Digest
}

func NewClient(ctx context.Context, h host.Host, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	s, err := h.NewStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		stream: s,
		engine: eng,
		digest: verifier.NewSHA256Digest(),
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
	for i, chunkID := range chunkIDs {
		// 1. Request Chunk
		req := BuildRequestChunk(chunkID)
		if err := WriteMessage(c.stream, req); err != nil {
			return fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
		}

		// 2. Receive Chunk
		resp, err := ReadMessage(c.stream)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		if resp.Type == MsgError {
			code, msg, _ := ParseError(resp.Payload)
			return fmt.Errorf("remote error on chunk %d (code %d): %s", i, code, msg)
		}

		if resp.Type != MsgChunk {
			return fmt.Errorf("expected CHUNK, got %d", resp.Type)
		}

		chunk, err := ParseChunk(resp.Payload)
		if err != nil {
			return fmt.Errorf("failed to parse chunk: %w", err)
		}

		// 3. Verify Hash matches ChunkID
		hash := c.digest.Sum(chunk.Data)
		if hash != core.Hash(chunkID) {
			// Notify remote peer of corruption
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
	}

	return nil
}
