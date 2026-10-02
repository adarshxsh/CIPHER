package availability

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	verification "cipher/availability/availability-contracts/verification"
	payment "cipher/availability/escrow-payment/payment"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

)

func TestAvailabilityProtocol_P2PStreamFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Setup mock libp2p network
	mockNet := mocknet.New()
	pubHost, err := mockNet.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate publisher host: %v", err)
	}
	provHost, err := mockNet.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate provider host: %v", err)
	}
	if err := mockNet.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	// 2. Setup Provider Proof Engine
	provEdPub, provEdPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate provider Ed25519 key: %v", err)
	}
	providerID := provHost.ID().String()

	proofEngine, err := NewProviderProofEngine(providerID, provEdPriv, nil)
	if err != nil {
		t.Fatalf("failed to create proof engine: %v", err)
	}

	// Chunks for test file
	chunks := [][]byte{
		[]byte("payload-segment-zero"),
		[]byte("payload-segment-one"),
		[]byte("payload-segment-two"),
		[]byte("payload-segment-three"),
	}
	fileID := "p2p-test-file-001"
	tree, err := proofEngine.RegisterFileChunks(fileID, chunks)
	if err != nil {
		t.Fatalf("failed to register file chunks: %v", err)
	}
	merkleRoot := tree.Root()

	// Register Provider Stream Handler
	voucherReceivedChan := make(chan bool, 1)
	handler := NewAvailabilityStreamHandler(provHost, proofEngine, func(voucher payment.PaymentState) {
		voucherReceivedChan <- true
	})
	_ = handler

	// 3. Setup Publisher Coordinator Bridge
	pubEdPub, pubEdPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate publisher Ed25519 key: %v", err)
	}
	_ = pubEdPub

	pubEthPriv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate publisher Ethereum key: %v", err)
	}
	publisherID := pubHost.ID().String()

	bridge, err := NewCoordinatorBridge(CoordinatorBridgeConfig{
		PublisherPeerID: publisherID,
		PublisherEthKey: pubEthPriv,
		PublisherEdKey:  pubEdPriv,
	})
	if err != nil {
		t.Fatalf("failed to create coordinator bridge: %v", err)
	}

	provEthPriv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate provider Eth key: %v", err)
	}
	provEthAddr := crypto.PubkeyToAddress(provEthPriv.PublicKey)

	if err := bridge.RegisterProvider(providerID, provEthAddr); err != nil {
		t.Fatalf("register provider in bridge failed: %v", err)
	}

	contractID := availabilitytypes.ContractID("p2p-contract-999")
	var escrowID [32]byte
	rand.Read(escrowID[:])

	if err := bridge.RegisterContractSchedule(contractID, escrowID, 50, 500); err != nil {
		t.Fatalf("register contract schedule failed: %v", err)
	}

	// 4. Publisher executes challenge over P2P
	client := NewAvailabilityClient(pubHost)

	challenge := availabilitytypes.Challenge{
		ChallengeID: "challenge-round-1",
		ContractID:  contractID,
		ProviderID:  providerID,
		FileID:      fileID,
		ChunkID:     1, // chunk 1
		Nonce:       []byte("fresh-p2p-nonce-555"),
		CreatedAt:   time.Now().UTC(),
	}

	resp, err := client.ChallengeProvider(ctx, provHost.ID(), challenge)
	if err != nil {
		t.Fatalf("ChallengeProvider failed: %v", err)
	}

	// 5. Publisher verifies cryptographic response
	valid, err := verification.VerifyChallengeResponse(
		challenge,
		resp,
		merkleRoot,
		provEdPub,
		15*time.Second,
	)
	if err != nil {
		t.Fatalf("VerifyChallengeResponse returned error: %v", err)
	}
	if !valid {
		t.Fatalf("expected challenge response to be valid, but got false")
	}

	// 6. Publisher coordinator dispatches PASS -> produces signed voucher
	dispatchRes, err := bridge.DispatchAvailabilityResult(availabilitytypes.AvailabilityResult{
		ContractID:  contractID,
		ProviderID:  providerID,
		Period:      1,
		ChallengeID: challenge.ChallengeID,
		Result:      availabilitytypes.AvailabilityPass,
		Timestamp:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("DispatchAvailabilityResult failed: %v", err)
	}

	// 7. Publisher sends payment voucher back to provider over P2P
	if err := client.SendPaymentVoucher(ctx, provHost.ID(), dispatchRes.PaymentState); err != nil {
		t.Fatalf("SendPaymentVoucher failed: %v", err)
	}

	select {
	case <-voucherReceivedChan:
		// Success!
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for voucher receipt callback on provider")
	}

	vouchers := handler.Vouchers()
	if len(vouchers) != 1 {
		t.Fatalf("expected 1 voucher on provider, got %d", len(vouchers))
	}
	if vouchers[0].CumulativePayment != 50 {
		t.Fatalf("expected cumulative payment 50, got %d", vouchers[0].CumulativePayment)
	}
}
