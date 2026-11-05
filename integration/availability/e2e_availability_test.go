package availability

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	contract "cipher/availability/availability-contracts/contract"
	availabilitytypes "cipher/availability/availability-contracts/types"
	coordinator "cipher/availability/escrow-payment/coordinator"
	payment "cipher/availability/escrow-payment/payment"
	"cipher/network/content/core"

	"cipher/network/content/crypto"
	"cipher/network/content/engine"
	"cipher/network/content/manifest"
	"cipher/network/content/storage"
	"cipher/network/content/verifier"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
	"proof-of-request/request"
	"proof-of-request/security"
)

func TestEndToEnd_AvailabilityIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// -------------------------------------------------------------------------
	// 1. SETUP MOCK P2P NETWORK
	// -------------------------------------------------------------------------
	mockNet := mocknet.New()
	pubHost, err := mockNet.GenPeer()
	if err != nil {
		t.Fatalf("failed to create publisher host: %v", err)
	}
	provHost, err := mockNet.GenPeer()
	if err != nil {
		t.Fatalf("failed to create provider host: %v", err)
	}
	if err := mockNet.LinkAll(); err != nil {
		t.Fatalf("failed to link mocknet: %v", err)
	}

	publisherPeerID := pubHost.ID().String()
	providerPeerID := provHost.ID().String()

	// -------------------------------------------------------------------------
	// 2. DUAL IDENTITY SETUP
	// -------------------------------------------------------------------------
	// Publisher Keys
	pubEdPub, pubEdPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate publisher Ed25519 key: %v", err)
	}
	_ = pubEdPub

	pubEthPriv, err := ethcrypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate publisher Ethereum key: %v", err)
	}

	// Provider Keys
	provEdPub, provEdPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate provider Ed25519 key: %v", err)
	}

	provEthPriv, err := ethcrypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate provider Ethereum key: %v", err)
	}
	provEthAddr := ethcrypto.PubkeyToAddress(provEthPriv.PublicKey)

	// -------------------------------------------------------------------------
	// 3. STORAGE & CONTENT ENGINE (Ingest and distribute 4 chunks)
	// -------------------------------------------------------------------------
	pubStoreDir := t.TempDir()
	pubStore := storage.NewFSStore(pubStoreDir)

	provStoreDir := t.TempDir()
	provStore := storage.NewFSStore(provStoreDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	eng := engine.NewContentEngine(config, enc, dig, pubStore, pubStore, keys, pubStore)

	// Ingest 128KB payload (exactly 4 x 32KB chunks)
	payload := make([]byte, 128*1024)
	rand.Read(payload)

	m, err := eng.Ingest(ctx, bytes.NewReader(payload), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}
	_ = pubStore.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)
	_ = provStore.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	fileID := hex.EncodeToString(m.Descriptor.ID[:])

	// Read chunks from pubStore and copy to provStore
	rawChunks := make([][]byte, len(m.ChunkIDs))
	for i, chunkID := range m.ChunkIDs {
		chunk, err := pubStore.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("GetChunk %d failed: %v", i, err)
		}
		rawChunks[i] = chunk.Data
		if err := provStore.PutChunk(ctx, chunk); err != nil {
			t.Fatalf("PutChunk %d to provStore failed: %v", i, err)
		}
	}

	// -------------------------------------------------------------------------
	// 4. STORAGE RESOLVER & GLOBAL CONFIGURATION
	// -------------------------------------------------------------------------
	resolver, err := InstallStorageResolver(pubStore)
	if err != nil {
		t.Fatalf("InstallStorageResolver failed: %v", err)
	}
	resolver.RegisterManifest(m)

	chunkCount, err := resolver.ChunkCount(providerPeerID, fileID)
	if err != nil || chunkCount != 4 {
		t.Fatalf("resolver.ChunkCount = %d, %v; want 4", chunkCount, err)
	}

	// -------------------------------------------------------------------------
	// 5. PROVIDER PROOF ENGINE & AVAILABILITY WIRE HANDLER
	// -------------------------------------------------------------------------
	proofEngine, err := NewProviderProofEngine(providerPeerID, provEdPriv, provStore)
	if err != nil {
		t.Fatalf("NewProviderProofEngine failed: %v", err)
	}

	tree, err := proofEngine.RegisterFileChunks(fileID, rawChunks)
	if err != nil {
		t.Fatalf("RegisterFileChunks failed: %v", err)
	}
	merkleRoot := tree.Root()

	voucherReceived := make(chan bool, 1)
	wireHandler := NewAvailabilityStreamHandler(provHost, proofEngine, func(voucher payment.PaymentState) {
		voucherReceived <- true
	})

	_ = wireHandler

	// -------------------------------------------------------------------------
	// 6. PROOF OF REQUEST: CACHE ANNOUNCEMENT & DEMAND TRACKING
	// -------------------------------------------------------------------------
	demandMgr := NewDemandAndReplicaManager()
	ann, err := CreateSignedCacheAnnouncement(providerPeerID, provEdPriv, m, 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateSignedCacheAnnouncement failed: %v", err)
	}

	if err := demandMgr.IngestAnnouncement(ann, provEdPub); err != nil {
		t.Fatalf("IngestAnnouncement failed: %v", err)
	}

	replicaCount, err := demandMgr.GetReplicaCount(fileID, ann.Announcement.Version)
	if err != nil || replicaCount != 1 {
		t.Fatalf("replica count = %d, want 1", replicaCount)
	}

	clientReq, err := request.CreateRequest(fileID, "consumer-node-42")
	if err != nil {
		t.Fatalf("CreateRequest failed: %v", err)
	}
	pow := security.GeneratePoW(clientReq, 1)
	if _, err := demandMgr.ValidateAndTrackRequest(clientReq, pow, time.Hour); err != nil {
		t.Fatalf("ValidateAndTrackRequest failed: %v", err)
	}

	demandVal, err := demandMgr.CalculateDemand(fileID)
	if err != nil || demandVal < 1 {
		t.Fatalf("CalculateDemand = %d, want >= 1", demandVal)
	}

	// -------------------------------------------------------------------------
	// 7. AVAILABILITY CONTRACT CREATION & ESCROW SCHEDULE
	// -------------------------------------------------------------------------
	contractID, err := contract.CreateAvailabilityContract(publisherPeerID, providerPeerID, fileID, 1000, 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract failed: %v", err)
	}

	contractState, err := contract.FundAvailabilityContract(contractID, 1000)
	if err != nil || contractState != availabilitytypes.Agreed {
		t.Fatalf("FundAvailabilityContract failed: %v (state: %s)", err, contractState)
	}

	bridge, err := NewCoordinatorBridge(CoordinatorBridgeConfig{
		PublisherPeerID:  publisherPeerID,
		PublisherEthKey:  pubEthPriv,
		PublisherEdKey:   pubEdPriv,
		FailureThreshold: 3,
	})
	if err != nil {
		t.Fatalf("NewCoordinatorBridge failed: %v", err)
	}

	if err := bridge.RegisterProvider(providerPeerID, provEthAddr); err != nil {
		t.Fatalf("RegisterProvider failed: %v", err)
	}

	var escrowAgreementID [32]byte
	rand.Read(escrowAgreementID[:])

	if err := bridge.RegisterContractSchedule(contractID, escrowAgreementID, 250, 1000); err != nil {
		t.Fatalf("RegisterContractSchedule failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// 8. P2P CHALLENGE ROUND 1: HAPPY PATH (PASS)
	// -------------------------------------------------------------------------
	challengeID1, _, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("InitiateAvailabilityChallenge 1 failed: %v", err)
	}

	issuedChallenge1 := availabilitytypes.Challenge{
		ChallengeID: challengeID1,
		ContractID:  contractID,
		ProviderID:  providerPeerID,
		FileID:      fileID,
		ChunkID:     2, // challenge chunk index 2
		Nonce:       []byte("random-entropy-nonce-round-1"),
		CreatedAt:   time.Now().UTC(),
	}

	p2pClient := NewAvailabilityClient(pubHost)
	resp1, err := p2pClient.ChallengeProvider(ctx, provHost.ID(), issuedChallenge1)
	if err != nil {
		t.Fatalf("ChallengeProvider round 1 failed: %v", err)
	}

	nextState, res1, err := contract.VerifyAndRecordChallengeResponse(
		contractID,
		issuedChallenge1,
		resp1,
		merkleRoot,
		provEdPub,
		15*time.Second,
	)
	if err != nil {
		t.Fatalf("VerifyAndRecordChallengeResponse round 1 failed: %v", err)
	}
	if !res1.Succeeded {
		t.Fatalf("expected challenge round 1 to SUCCEED, got FAIL")
	}
	if nextState != availabilitytypes.Active {
		t.Fatalf("expected Active contract state, got %s", nextState)
	}

	latestRes1, err := contract.GetLatestAvailabilityResult(contractID)
	if err != nil {
		t.Fatalf("GetLatestAvailabilityResult failed: %v", err)
	}

	dispatch1, err := bridge.DispatchAvailabilityResult(latestRes1)
	if err != nil {
		t.Fatalf("DispatchAvailabilityResult 1 failed: %v", err)
	}
	if dispatch1.Kind != coordinator.DispatchKindPass {
		t.Fatalf("expected DispatchKindPass, got %s", dispatch1.Kind)
	}
	if dispatch1.PaymentState.CumulativePayment != 250 {
		t.Fatalf("expected cumulative payment 250, got %d", dispatch1.PaymentState.CumulativePayment)
	}

	// Stream signed payment voucher to provider
	if err := p2pClient.SendPaymentVoucher(ctx, provHost.ID(), dispatch1.PaymentState); err != nil {
		t.Fatalf("SendPaymentVoucher 1 failed: %v", err)
	}

	select {
	case <-voucherReceived:
		// Provider received voucher!
	case <-time.After(3 * time.Second):
		t.Fatalf("provider timed out waiting for payment voucher")
	}

	// -------------------------------------------------------------------------
	// 9. P2P CHALLENGE ROUND 2: ADVERSARIAL TAMPERING (FAIL)
	// -------------------------------------------------------------------------
	challengeID2, _, err := contract.InitiateAvailabilityChallenge(contractID)
	if err != nil {
		t.Fatalf("InitiateAvailabilityChallenge 2 failed: %v", err)
	}

	issuedChallenge2 := availabilitytypes.Challenge{
		ChallengeID: challengeID2,
		ContractID:  contractID,
		ProviderID:  providerPeerID,
		FileID:      fileID,
		ChunkID:     0,
		Nonce:       []byte("random-entropy-nonce-round-2"),
		CreatedAt:   time.Now().UTC(),
	}

	// Generate tampered response with bad chunk hash
	tamperedResp, err := proofEngine.HandleChallengeAdversarial(issuedChallenge2, true, false)
	if err != nil {
		t.Fatalf("HandleChallengeAdversarial failed: %v", err)
	}

	_, res2, err := contract.VerifyAndRecordChallengeResponse(
		contractID,
		issuedChallenge2,
		tamperedResp,
		merkleRoot,
		provEdPub,
		15*time.Second,
	)
	if err != nil {
		t.Fatalf("VerifyAndRecordChallengeResponse round 2 failed: %v", err)
	}
	if res2.Succeeded {
		t.Fatalf("expected tampered challenge round 2 to FAIL, but SUCCEEDED")
	}

	latestRes2, err := contract.GetLatestAvailabilityResult(contractID)
	if err != nil {
		t.Fatalf("GetLatestAvailabilityResult round 2 failed: %v", err)
	}

	dispatch2, err := bridge.DispatchAvailabilityResult(latestRes2)
	if err != nil {
		t.Fatalf("DispatchAvailabilityResult 2 failed: %v", err)
	}
	if dispatch2.Kind != coordinator.DispatchKindFail {
		t.Fatalf("expected DispatchKindFail, got %s", dispatch2.Kind)
	}
	if dispatch2.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 consecutive failure, got %d", dispatch2.ConsecutiveFailures)
	}

	// -------------------------------------------------------------------------
	// 10. FINAL CONTRACT SETTLEMENT
	// -------------------------------------------------------------------------
	settleStatus, err := contract.SettleAvailabilityContract(contractID)
	if err != nil {
		t.Fatalf("SettleAvailabilityContract failed: %v", err)
	}
	// Because 1 out of 2 challenges failed, the contract proportionally withholds funds
	if settleStatus != availabilitytypes.SettlementWithheld {
		t.Fatalf("settleStatus = %s, want %s", settleStatus, availabilitytypes.SettlementWithheld)
	}


	// Verify Provider's recorded vouchers
	vouchers := wireHandler.Vouchers()
	if len(vouchers) != 1 {
		t.Fatalf("expected provider to have stored 1 voucher, got %d", len(vouchers))
	}
	if vouchers[0].CumulativePayment != 250 {
		t.Fatalf("stored voucher cumulative payment = %d, want 250", vouchers[0].CumulativePayment)
	}
}
