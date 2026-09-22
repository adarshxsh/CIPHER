package chunk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

const (
	CurrentMessageVersion uint16 = 1
	MaxAttestationAgeSeconds int64 = 3600 // 1 hour max attestation age
)

type ProviderAttestation struct {
	ContentID      core.ContentID `json:"content_id"`
	ProviderID     string         `json:"provider_id"`
	ProviderPubKey []byte         `json:"provider_pub_key,omitempty"`
	Timestamp      int64          `json:"timestamp"`
	Signature      []byte         `json:"signature"`
}

func NewProviderAttestation(contentID core.ContentID, providerID peer.ID, timestamp int64, privKey crypto.PrivKey) (*ProviderAttestation, error) {
	att := &ProviderAttestation{
		ContentID:  contentID,
		ProviderID: providerID.String(),
		Timestamp:  timestamp,
	}
	if privKey != nil {
		pubKey := privKey.GetPublic()
		if pubBytes, err := crypto.MarshalPublicKey(pubKey); err == nil {
			att.ProviderPubKey = pubBytes
		}
		if err := att.Sign(privKey); err != nil {
			return nil, fmt.Errorf("failed to sign provider attestation: %w", err)
		}
	}
	return att, nil
}

func (a *ProviderAttestation) SignableBytes() ([]byte, error) {
	aCopy := *a
	aCopy.Signature = nil
	return json.Marshal(&aCopy)
}

func (a *ProviderAttestation) Sign(privKey crypto.PrivKey) error {
	if privKey == nil {
		return errors.New("private key cannot be nil")
	}
	a.Signature = nil
	bytesToSign, err := a.SignableBytes()
	if err != nil {
		return err
	}
	sig, err := privKey.Sign(bytesToSign)
	if err != nil {
		return err
	}
	a.Signature = sig
	return nil
}

func (a *ProviderAttestation) Verify(expectedContentID core.ContentID, expectedProvider peer.ID) error {
	if a == nil {
		return errors.New("provider attestation is nil")
	}
	if a.ContentID != expectedContentID {
		return fmt.Errorf("attestation content ID mismatch: expected %x, got %x", expectedContentID, a.ContentID)
	}
	if a.ProviderID == "" || len(a.Signature) == 0 {
		return errors.New("missing provider ID or signature in attestation")
	}

	providerPeerID, err := peer.Decode(a.ProviderID)
	if err != nil {
		return fmt.Errorf("invalid provider peer ID in attestation: %w", err)
	}

	if expectedProvider != "" && providerPeerID != expectedProvider {
		return fmt.Errorf("provider peer ID mismatch: expected %s, got %s", expectedProvider, providerPeerID)
	}

	// Verify timestamp freshness
	now := time.Now().Unix()
	diff := now - a.Timestamp
	if diff > MaxAttestationAgeSeconds || diff < -300 { // allow 5m clock skew
		return fmt.Errorf("provider attestation timestamp expired or invalid: age=%ds", diff)
	}

	// Extract public key or derive from ProviderPubKey
	var pubKey crypto.PubKey
	var keyErr error
	pubKey, keyErr = providerPeerID.ExtractPublicKey()
	if keyErr != nil || pubKey == nil {
		if len(a.ProviderPubKey) > 0 {
			pubKey, keyErr = manifest.GetCachedPubKey(a.ProviderPubKey)
			if keyErr != nil {
				return fmt.Errorf("failed to parse provider public key: %w", keyErr)
			}
			derivedID, err := peer.IDFromPublicKey(pubKey)
			if err != nil || derivedID != providerPeerID {
				return fmt.Errorf("provider public key does not match peer ID")
			}
		} else {
			return fmt.Errorf("failed to extract public key from provider peer ID: %w", keyErr)
		}
	}

	signableBytes, err := a.SignableBytes()
	if err != nil {
		return fmt.Errorf("failed to compute signable bytes for attestation: %w", err)
	}

	ok, err := pubKey.Verify(signableBytes, a.Signature)
	if err != nil || !ok {
		return fmt.Errorf("provider attestation signature verification failed")
	}

	return nil
}

type MessageType uint8

const (
	MsgRequestManifest MessageType = 0x01
	MsgManifest        MessageType = 0x02
	MsgRequestChunk    MessageType = 0x03
	MsgChunk           MessageType = 0x04
	MsgAck             MessageType = 0x05
	MsgError           MessageType = 0x06
)

type ErrorCode uint8

const (
	ErrContentNotFound    ErrorCode = 0x01
	ErrChunkNotFound      ErrorCode = 0x02
	ErrInvalidManifest    ErrorCode = 0x03
	ErrCodePermissionDenied ErrorCode = 0x04
	ErrInternal           ErrorCode = 0x05
	ErrIntegrityMismatch  ErrorCode = 0x06
	ErrBadRequest         ErrorCode = 0x07
	ErrUnsupportedMessage ErrorCode = 0x08
)

var (
	ErrPermissionDenied = errors.New("permission denied")
)

// Message is the symmetric envelope for all protocol communications.
type Message struct {
	Version uint16
	Type    MessageType
	Payload []byte
}

func WriteMessage(w io.Writer, msg *Message) error {
	buf := new(bytes.Buffer)
	
	// Envelope: Version (2), Type (1)
	if err := binary.Write(buf, binary.LittleEndian, msg.Version); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, msg.Type); err != nil {
		return err
	}
	// Payload
	buf.Write(msg.Payload)

	// Frame: Size prefix (4 bytes)
	size := uint32(buf.Len())
	if err := binary.Write(w, binary.LittleEndian, size); err != nil {
		return err
	}

	_, err := w.Write(buf.Bytes())
	return err
}

func ReadMessage(r io.Reader) (*Message, error) {
	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	if size > 2*1024*1024 { // 2MB max frame size
		return nil, errors.New("message exceeds maximum frame size")
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	buf := bytes.NewReader(data)
	msg := &Message{}
	if err := binary.Read(buf, binary.LittleEndian, &msg.Version); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &msg.Type); err != nil {
		return nil, err
	}

	msg.Payload = make([]byte, buf.Len())
	buf.Read(msg.Payload)

	return msg, nil
}

// -- Payload Builders & Parsers --

func BuildRequestManifest(id core.ContentID) *Message {
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgRequestManifest,
		Payload: id[:],
	}
}

func ParseRequestManifest(payload []byte) (core.ContentID, error) {
	var id core.ContentID
	if len(payload) != 32 {
		return id, fmt.Errorf("invalid payload length for REQUEST_MANIFEST: %d", len(payload))
	}
	copy(id[:], payload)
	return id, nil
}

func BuildManifest(id core.ContentID, att *ProviderAttestation, data []byte) *Message {
	var attBytes []byte
	if att != nil {
		attBytes, _ = json.Marshal(att)
	}
	attLen := uint32(len(attBytes))

	buf := new(bytes.Buffer)
	buf.Write(id[:])
	binary.Write(buf, binary.LittleEndian, attLen)
	if attLen > 0 {
		buf.Write(attBytes)
	}
	buf.Write(data)

	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgManifest,
		Payload: buf.Bytes(),
	}
}

func ParseManifest(payload []byte) (core.ContentID, *ProviderAttestation, []byte, error) {
	var id core.ContentID
	if len(payload) < 32 {
		return id, nil, nil, fmt.Errorf("invalid payload length for MANIFEST: %d", len(payload))
	}
	copy(id[:], payload[:32])

	if len(payload) < 36 {
		return id, nil, payload[32:], nil
	}

	attLen := binary.LittleEndian.Uint32(payload[32:36])
	if attLen == 0 {
		return id, nil, payload[36:], nil
	}

	if int(36+attLen) > len(payload) {
		return id, nil, payload[32:], nil
	}

	var att ProviderAttestation
	attBytes := payload[36 : 36+attLen]
	if err := json.Unmarshal(attBytes, &att); err != nil {
		return id, nil, nil, fmt.Errorf("invalid provider attestation in MANIFEST: %w", err)
	}

	return id, &att, payload[36+attLen:], nil
}

func BuildRequestChunk(id core.ChunkID) *Message {
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgRequestChunk,
		Payload: id[:],
	}
}

func ParseRequestChunk(payload []byte) (core.ChunkID, error) {
	var id core.ChunkID
	if len(payload) != 32 {
		return id, fmt.Errorf("invalid payload length for REQUEST_CHUNK: %d", len(payload))
	}
	copy(id[:], payload)
	return id, nil
}

func BuildChunk(chunk *core.Chunk) (*Message, error) {
	buf := new(bytes.Buffer)
	// Write Header
	if err := binary.Write(buf, binary.LittleEndian, &chunk.Header); err != nil {
		return nil, err
	}
	// Write Ciphertext
	buf.Write(chunk.Data)
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgChunk,
		Payload: buf.Bytes(),
	}, nil
}

func ParseChunk(payload []byte) (*core.Chunk, error) {
	chunk := &core.Chunk{}
	buf := bytes.NewReader(payload)
	if err := binary.Read(buf, binary.LittleEndian, &chunk.Header); err != nil {
		return nil, err
	}
	chunk.Data = make([]byte, buf.Len())
	buf.Read(chunk.Data)
	return chunk, nil
}

func BuildAck(id core.ChunkID, status uint8) *Message {
	payload := append(id[:], status)
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgAck,
		Payload: payload,
	}
}

func ParseAck(payload []byte) (core.ChunkID, uint8, error) {
	var id core.ChunkID
	if len(payload) != 33 {
		return id, 0, fmt.Errorf("invalid payload length for ACK: %d", len(payload))
	}
	copy(id[:], payload[:32])
	return id, payload[32], nil
}

func BuildError(code ErrorCode, msg string) *Message {
	payload := append([]byte{byte(code)}, []byte(msg)...)
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgError,
		Payload: payload,
	}
}

func ParseError(payload []byte) (ErrorCode, string, error) {
	if len(payload) < 1 {
		return 0, "", errors.New("invalid payload length for ERROR")
	}
	return ErrorCode(payload[0]), string(payload[1:]), nil
}
