package chunk

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/transport"
)

type Client struct {
	stream network.Stream
	engine *engine.ContentEngine
	digest core.Digest
}

// NewClient creates a new chunk client that communicates with a remote peer over the chunk transport protocol.
func NewClient(ctx context.Context, t *transport.Transport, peerID peer.ID, eng *engine.ContentEngine) (*Client, error) {
	stream, err := t.OpenStream(ctx, peerID, protocol.ChunkTransportProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		stream: stream,
		engine: eng,
		digest: verifier.NewSHA256Digest(),
	}, nil
}

func (c *Client) Close() error {
	return c.stream.Close()
}

// Resolve requests the manifest for a given content ID from the remote peer and returns the raw manifest data.
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

	manifestResp, err := ParseManifest(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse manifest response: %w", err)
	}

	if manifestResp.ContentID != id {
		return nil, fmt.Errorf("content ID mismatch in response: expected %x, got %x", id, manifestResp.ContentID)
	}

	remotePeerID := c.stream.Conn().RemotePeer()
	if manifestResp.ProviderID != remotePeerID {
		return nil, fmt.Errorf("provider peer ID mismatch: expected %s, got %s", remotePeerID, manifestResp.ProviderID)
	}

	now := time.Now().Unix()
	diff := now - manifestResp.Timestamp
	if diff > MaxAttestationAgeSeconds || diff < -MaxAttestationAgeSeconds {
		return nil, fmt.Errorf("manifest ownership attestation timestamp expired or in future: timestamp=%d, current=%d", manifestResp.Timestamp, now)
	}

	pubKey := c.stream.Conn().RemotePublicKey()
	if pubKey == nil {
		var err error
		pubKey, err = remotePeerID.ExtractPublicKey()
		if err != nil {
			return nil, fmt.Errorf("failed to extract public key for remote peer %s: %w", remotePeerID, err)
		}
	}
	if pubKey == nil {
		return nil, fmt.Errorf("failed to obtain public key for remote peer %s", remotePeerID)
	}

	attestationData := FormatAttestationData(manifestResp.ContentID, manifestResp.ProviderID, manifestResp.Timestamp)
	valid, err := pubKey.Verify(attestationData, manifestResp.Signature)
	if err != nil || !valid {
		return nil, fmt.Errorf("provider ownership attestation signature verification failed for peer %s", remotePeerID)
	}

	return manifestResp.Data, nil
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
	req := BuildRequestChunk(chunkID)
	if err := WriteMessage(c.stream, req); err != nil {
		return nil, fmt.Errorf("failed to send REQUEST_CHUNK: %w", err)
	}

	resp, err := ReadMessage(c.stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.Type == MsgError {
		code, msg, _ := ParseError(resp.Payload)
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
		WriteMessage(c.stream, errMsg)
		return nil, fmt.Errorf("corrupted chunk %x received", chunkID)
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
