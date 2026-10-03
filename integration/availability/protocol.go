package availability

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	verification "cipher/availability/availability-contracts/verification"
	payment "cipher/availability/escrow-payment/payment"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2pprotocol "github.com/libp2p/go-libp2p/core/protocol"
)

const (
	// AvailabilityProtocolID is the libp2p protocol identifier for availability challenges.
	AvailabilityProtocolID libp2pprotocol.ID = "/cipher/availability/1.0.0"

	MaxAvailabilityMessageSize uint32 = 64 * 1024 // 64 KB

	MsgChallengeRequest  byte = 0x20
	MsgChallengeResponse byte = 0x21
	MsgPaymentVoucher    byte = 0x22
	MsgChallengeError    byte = 0x23

	DefaultWireTimeout = 15 * time.Second
)

type WireEnvelope struct {
	Type    byte
	Payload []byte
}

func WriteEnvelope(w io.Writer, env *WireEnvelope) error {
	totalLen := uint32(1 + len(env.Payload))
	if totalLen > MaxAvailabilityMessageSize {
		return fmt.Errorf("envelope size %d exceeds limit %d", totalLen, MaxAvailabilityMessageSize)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, totalLen); err != nil {
		return err
	}
	buf.WriteByte(env.Type)
	buf.Write(env.Payload)

	_, err := w.Write(buf.Bytes())
	return err
}

func ReadEnvelope(r io.Reader) (*WireEnvelope, error) {
	var totalLen uint32
	if err := binary.Read(r, binary.LittleEndian, &totalLen); err != nil {
		return nil, err
	}
	if totalLen > MaxAvailabilityMessageSize {
		return nil, fmt.Errorf("message size %d exceeds limit %d", totalLen, MaxAvailabilityMessageSize)
	}
	if totalLen < 1 {
		return nil, errors.New("empty envelope frame")
	}

	data := make([]byte, totalLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return &WireEnvelope{
		Type:    data[0],
		Payload: data[1:],
	}, nil
}

// AvailabilityStreamHandler runs on provider nodes, answering challenges and collecting vouchers.
type AvailabilityStreamHandler struct {
	host        host.Host
	proofEngine *ProviderProofEngine
	mu          sync.RWMutex
	onVoucher   func(voucher payment.PaymentState)
	vouchers    []payment.PaymentState
}

// NewAvailabilityStreamHandler registers the /cipher/availability/1.0.0 handler on a libp2p host.
func NewAvailabilityStreamHandler(h host.Host, proofEngine *ProviderProofEngine, onVoucher func(voucher payment.PaymentState)) *AvailabilityStreamHandler {
	handler := &AvailabilityStreamHandler{
		host:        h,
		proofEngine: proofEngine,
		onVoucher:   onVoucher,
		vouchers:    make([]payment.PaymentState, 0),
	}
	h.SetStreamHandler(AvailabilityProtocolID, handler.HandleStream)
	return handler
}

func (h *AvailabilityStreamHandler) HandleStream(s network.Stream) {
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(DefaultWireTimeout))

	env, err := ReadEnvelope(s)
	if err != nil {
		return
	}

	switch env.Type {
	case MsgChallengeRequest:
		var challenge availabilitytypes.Challenge
		if err := json.Unmarshal(env.Payload, &challenge); err != nil {
			_ = WriteEnvelope(s, &WireEnvelope{
				Type:    MsgChallengeError,
				Payload: []byte(fmt.Sprintf(`{"error":"bad json: %v"}`, err)),
			})
			return
		}

		resp, err := h.proofEngine.HandleChallenge(challenge)
		if err != nil {
			_ = WriteEnvelope(s, &WireEnvelope{
				Type:    MsgChallengeError,
				Payload: []byte(fmt.Sprintf(`{"error":"proof failed: %v"}`, err)),
			})
			return
		}

		payloadBytes, err := json.Marshal(resp)
		if err != nil {
			return
		}

		_ = WriteEnvelope(s, &WireEnvelope{
			Type:    MsgChallengeResponse,
			Payload: payloadBytes,
		})

	case MsgPaymentVoucher:
		var voucher payment.PaymentState
		if err := json.Unmarshal(env.Payload, &voucher); err == nil {
			h.mu.Lock()
			h.vouchers = append(h.vouchers, voucher)
			callback := h.onVoucher
			h.mu.Unlock()

			if callback != nil {
				callback(voucher)
			}
		}
	}
}

// Vouchers returns all payment vouchers received over P2P streams.
func (h *AvailabilityStreamHandler) Vouchers() []payment.PaymentState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	res := make([]payment.PaymentState, len(h.vouchers))
	copy(res, h.vouchers)
	return res
}

// AvailabilityClient allows a publisher to execute challenges and send vouchers over libp2p.
type AvailabilityClient struct {
	host host.Host
}

// NewAvailabilityClient creates an AvailabilityClient for libp2p peer interactions.
func NewAvailabilityClient(h host.Host) *AvailabilityClient {
	return &AvailabilityClient{host: h}
}

// ChallengeProvider sends a challenge to a remote provider over /cipher/availability/1.0.0
// and waits for its cryptographic response.
func (c *AvailabilityClient) ChallengeProvider(
	ctx context.Context,
	providerPeerID peer.ID,
	challenge availabilitytypes.Challenge,
) (verification.ChallengeResponse, error) {
	s, err := c.host.NewStream(ctx, providerPeerID, AvailabilityProtocolID)
	if err != nil {
		return verification.ChallengeResponse{}, fmt.Errorf("open availability stream: %w", err)
	}
	defer s.Close()

	_ = s.SetDeadline(time.Now().Add(DefaultWireTimeout))

	payload, err := json.Marshal(challenge)
	if err != nil {
		return verification.ChallengeResponse{}, fmt.Errorf("marshal challenge: %w", err)
	}

	if err := WriteEnvelope(s, &WireEnvelope{
		Type:    MsgChallengeRequest,
		Payload: payload,
	}); err != nil {
		return verification.ChallengeResponse{}, fmt.Errorf("send challenge: %w", err)
	}

	env, err := ReadEnvelope(s)
	if err != nil {
		return verification.ChallengeResponse{}, fmt.Errorf("read response envelope: %w", err)
	}

	if env.Type == MsgChallengeError {
		return verification.ChallengeResponse{}, fmt.Errorf("provider returned error: %s", string(env.Payload))
	}

	if env.Type != MsgChallengeResponse {
		return verification.ChallengeResponse{}, fmt.Errorf("unexpected envelope type 0x%x", env.Type)
	}

	var resp verification.ChallengeResponse
	if err := json.Unmarshal(env.Payload, &resp); err != nil {
		return verification.ChallengeResponse{}, fmt.Errorf("unmarshal challenge response: %w", err)
	}

	return resp, nil
}

// SendPaymentVoucher delivers a signed off-chain payment voucher to the provider.
func (c *AvailabilityClient) SendPaymentVoucher(
	ctx context.Context,
	providerPeerID peer.ID,
	voucher payment.PaymentState,
) error {
	s, err := c.host.NewStream(ctx, providerPeerID, AvailabilityProtocolID)
	if err != nil {
		return fmt.Errorf("open stream for voucher: %w", err)
	}
	defer s.Close()

	_ = s.SetDeadline(time.Now().Add(DefaultWireTimeout))

	payload, err := json.Marshal(voucher)
	if err != nil {
		return fmt.Errorf("marshal voucher: %w", err)
	}

	return WriteEnvelope(s, &WireEnvelope{
		Type:    MsgPaymentVoucher,
		Payload: payload,
	})
}
