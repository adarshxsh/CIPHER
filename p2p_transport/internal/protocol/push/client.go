package push

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/transport"
)

type Client struct {
	stream network.Stream
	peerID peer.ID
}

func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID) (*Client, error) {
	stream, err := t.OpenStream(ctx, peerID, protocol.PushTransportProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open push stream to %s: %w", peerID, err)
	}

	return &Client{
		stream: stream,
		peerID: peerID,
	}, nil
}

func (c *Client) Close() error {
	return c.stream.Close()
}

func (c *Client) PeerID() peer.ID {
	return c.peerID
}

// SendManifest sends the manifest metadata along with the exact set of assigned chunk IDs for this provider.
func (c *Client) SendManifest(ctx context.Context, contentID core.ContentID, assignedChunkIDs []core.ChunkID, manifestData []byte) error {
	var deadline time.Time
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else {
		deadline = time.Now().Add(WriteTimeout + ReadTimeout)
	}
	_ = c.stream.SetDeadline(deadline)
	defer c.stream.SetDeadline(time.Time{})

	stop := context.AfterFunc(ctx, func() {
		_ = c.stream.Reset()
	})
	defer stop()

	req := BuildPushManifest(contentID, assignedChunkIDs, manifestData)
	if err := WritePushMessage(c.stream, req); err != nil {
		_ = c.stream.Reset()
		if ctx.Err() != nil {
			return fmt.Errorf("failed to write PUSH_MANIFEST: %w", ctx.Err())
		}
		return fmt.Errorf("failed to write PUSH_MANIFEST: %w", err)
	}

	resp, err := ReadPushMessage(c.stream)
	if err != nil {
		_ = c.stream.Reset()
		if ctx.Err() != nil {
			return fmt.Errorf("failed to read PUSH_MANIFEST_ACK: %w", ctx.Err())
		}
		return fmt.Errorf("failed to read PUSH_MANIFEST_ACK: %w", err)
	}

	if resp.Type == MsgPushError {
		code, msg, _ := ParsePushError(resp.Payload)
		return fmt.Errorf("remote push error (code %d): %s", code, msg)
	}

	if resp.Type != MsgPushManifestAck {
		return fmt.Errorf("unexpected response type: %d, expected PUSH_MANIFEST_ACK", resp.Type)
	}

	respID, status, err := ParsePushManifestAck(resp.Payload)
	if err != nil {
		return fmt.Errorf("failed to parse PUSH_MANIFEST_ACK: %w", err)
	}

	if respID != contentID {
		return fmt.Errorf("contentID mismatch in manifest ack: expected %x, got %x", contentID, respID)
	}

	if status != PushStatusOK {
		return fmt.Errorf("provider rejected manifest with status code %d", status)
	}

	return nil
}

// SendChunk transmits a single chunk to the provider and waits for confirmation.
func (c *Client) SendChunk(ctx context.Context, contentID core.ContentID, chunk *core.Chunk) error {
	var deadline time.Time
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else {
		deadline = time.Now().Add(WriteTimeout + AckTimeout)
	}
	_ = c.stream.SetDeadline(deadline)
	defer c.stream.SetDeadline(time.Time{})

	stop := context.AfterFunc(ctx, func() {
		_ = c.stream.Reset()
	})
	defer stop()

	req, err := BuildPushChunk(contentID, chunk)
	if err != nil {
		return fmt.Errorf("failed to build PUSH_CHUNK: %w", err)
	}

	if err := WritePushMessage(c.stream, req); err != nil {
		_ = c.stream.Reset()
		if ctx.Err() != nil {
			return fmt.Errorf("failed to write PUSH_CHUNK: %w", ctx.Err())
		}
		return fmt.Errorf("failed to write PUSH_CHUNK: %w", err)
	}

	resp, err := ReadPushMessage(c.stream)
	if err != nil {
		_ = c.stream.Reset()
		if ctx.Err() != nil {
			return fmt.Errorf("failed to read PUSH_CHUNK_ACK: %w", ctx.Err())
		}
		return fmt.Errorf("failed to read PUSH_CHUNK_ACK: %w", err)
	}

	if resp.Type == MsgPushError {
		code, msg, _ := ParsePushError(resp.Payload)
		return fmt.Errorf("remote push error (code %d): %s", code, msg)
	}

	if resp.Type != MsgPushChunkAck {
		return fmt.Errorf("unexpected response type: %d, expected PUSH_CHUNK_ACK", resp.Type)
	}

	ackChunkID, status, err := ParsePushChunkAck(resp.Payload)
	if err != nil {
		return fmt.Errorf("failed to parse PUSH_CHUNK_ACK: %w", err)
	}

	if ackChunkID != chunk.Header.ID {
		return fmt.Errorf("chunkID mismatch in chunk ack: expected %x, got %x", chunk.Header.ID, ackChunkID)
	}

	if status != PushStatusOK {
		return fmt.Errorf("provider rejected chunk %x with status code %d", chunk.Header.ID, status)
	}

	return nil
}

// SendBatchComplete signals to the provider that all assigned chunks have been uploaded.
func (c *Client) SendBatchComplete(ctx context.Context, contentID core.ContentID) error {
	var deadline time.Time
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else {
		deadline = time.Now().Add(WriteTimeout + ReadTimeout)
	}
	_ = c.stream.SetDeadline(deadline)
	defer c.stream.SetDeadline(time.Time{})

	stop := context.AfterFunc(ctx, func() {
		_ = c.stream.Reset()
	})
	defer stop()

	req := BuildPushBatchComplete(contentID)
	if err := WritePushMessage(c.stream, req); err != nil {
		_ = c.stream.Reset()
		if ctx.Err() != nil {
			return fmt.Errorf("failed to write PUSH_BATCH_COMPLETE: %w", ctx.Err())
		}
		return fmt.Errorf("failed to write PUSH_BATCH_COMPLETE: %w", err)
	}

	resp, err := ReadPushMessage(c.stream)
	if err != nil {
		_ = c.stream.Reset()
		if ctx.Err() != nil {
			return fmt.Errorf("failed to read PUSH_BATCH_COMPLETE_ACK: %w", ctx.Err())
		}
		return fmt.Errorf("failed to read PUSH_BATCH_COMPLETE_ACK: %w", err)
	}

	if resp.Type == MsgPushError {
		code, msg, _ := ParsePushError(resp.Payload)
		return fmt.Errorf("remote push error (code %d): %s", code, msg)
	}

	if resp.Type != MsgPushBatchCompleteAck {
		return fmt.Errorf("unexpected response type: %d, expected PUSH_BATCH_COMPLETE_ACK", resp.Type)
	}

	respID, status, err := ParsePushBatchCompleteAck(resp.Payload)
	if err != nil {
		return fmt.Errorf("failed to parse PUSH_BATCH_COMPLETE_ACK: %w", err)
	}

	if respID != contentID {
		return fmt.Errorf("contentID mismatch in batch complete ack: expected %x, got %x", contentID, respID)
	}

	if status != PushStatusOK {
		return fmt.Errorf("provider rejected batch completion with status code %d", status)
	}

	return nil
}
