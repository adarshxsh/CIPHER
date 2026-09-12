package chunk

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
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

	respID, att, data, err := ParseManifest(resp.Payload)
	if err != nil {
		return nil, err
	}
	if respID != id {
		log.Printf("[Security] Content ID mismatch in manifest response from peer %s: expected %x, got %x", c.stream.Conn().RemotePeer(), id, respID)
		c.stream.Reset()
		return nil, fmt.Errorf("content ID mismatch in response")
	}

	if att == nil {
		log.Printf("[Security] Missing provider attestation in manifest response from peer %s", c.stream.Conn().RemotePeer())
		c.stream.Reset()
		return nil, fmt.Errorf("missing provider attestation in manifest response")
	}

	if att.ContentID != id {
		log.Printf("[Security] Provider attestation ContentID mismatch from peer %s: expected %x, got %x", c.stream.Conn().RemotePeer(), id, att.ContentID)
		c.stream.Reset()
		return nil, fmt.Errorf("provider attestation ContentID mismatch")
	}

	remotePeerID := c.stream.Conn().RemotePeer()
	if att.ProviderID != remotePeerID.String() {
		log.Printf("[Security] Provider ID mismatch in attestation from peer %s: attestation has %s", remotePeerID, att.ProviderID)
		c.stream.Reset()
		return nil, fmt.Errorf("provider ID mismatch in attestation: %s != %s", att.ProviderID, remotePeerID)
	}

	now := time.Now().Unix()
	skew := now - att.Timestamp
	if skew < -300 || skew > 300 {
		log.Printf("[Security] Provider attestation timestamp skew out of bounds from peer %s: skew=%d seconds", remotePeerID, skew)
		c.stream.Reset()
		return nil, fmt.Errorf("provider attestation timestamp skew out of bounds: %d seconds", skew)
	}

	var pubKey crypto.PubKey
	if len(att.ProviderPubKey) > 0 {
		var err error
		pubKey, err = GetCachedPubKey(att.ProviderPubKey)
		if err != nil {
			log.Printf("[Security] Invalid provider public key from peer %s: %v", remotePeerID, err)
			c.stream.Reset()
			return nil, fmt.Errorf("invalid provider public key: %w", err)
		}
		pubKeyPeerID, err := peer.IDFromPublicKey(pubKey)
		if err != nil || pubKeyPeerID != remotePeerID {
			log.Printf("[Security] Provider public key does not match remote peer ID %s", remotePeerID)
			c.stream.Reset()
			return nil, fmt.Errorf("provider public key does not match remote peer ID")
		}
	} else {
		pubKey = c.stream.Conn().RemotePublicKey()
		if pubKey == nil {
			log.Printf("[Security] Transport connection for peer %s missing remote public key", remotePeerID)
			c.stream.Reset()
			return nil, fmt.Errorf("missing remote public key on transport connection")
		}
	}

	if err := att.Verify(pubKey); err != nil {
		log.Printf("[Security] Provider attestation signature verification failed for peer %s: %v", remotePeerID, err)
		c.stream.Reset()
		return nil, fmt.Errorf("provider attestation signature verification failed: %w", err)
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
