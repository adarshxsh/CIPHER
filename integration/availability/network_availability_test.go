package availability

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	verification "cipher/availability/availability-contracts/verification"
	coordinator "cipher/availability/escrow-payment/coordinator"
	payment "cipher/availability/escrow-payment/payment"
	"cipher/network/content/core"
	"cipher/network/content/manifest"
	"cipher/network/content/storage"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// helper to create a real TCP libp2p node listening on a dynamic loopback port
func createRealTCPNode(t *testing.T, ctx context.Context) (host.Host, ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()

	edPub, edPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ed25519 key: %v", err)
	}

	p2pPriv, err := libp2pcrypto.UnmarshalEd25519PrivateKey(edPriv)
	if err != nil {
		t.Fatalf("failed to convert to libp2p private key: %v", err)
	}

	h, err := libp2p.New(
		libp2p.Identity(p2pPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatalf("failed to create real TCP libp2p node: %v", err)
	}

	return h, edPriv, edPub
}

// TestAvailabilityNetwork_PublisherProviderFullSuite executes a comprehensive
// network-only integration suite for the Availability Subsystem between a Publisher
// and multiple real TCP Providers over libp2p (/cipher/availability/1.0.0).
func TestAvailabilityNetwork_PublisherProviderFullSuite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// -------------------------------------------------------------
	// 1. SETUP PUBLISHER NODE (REAL TCP)
	// -------------------------------------------------------------
	pubHost, pubEdPriv, pubEdPub := createRealTCPNode(t, ctx)
	defer pubHost.Close()

	pubEthPriv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate publisher Ethereum key: %v", err)
	}

	publisherID := pubHost.ID().String()
	coordBridge, err := NewCoordinatorBridge(CoordinatorBridgeConfig{
		PublisherPeerID: publisherID,
		PublisherEthKey: pubEthPriv,
		PublisherEdKey:  pubEdPriv,
	})
	if err != nil {
		t.Fatalf("failed to initialize coordinator bridge: %v", err)
	}

	pubClient := NewAvailabilityClient(pubHost)

	// -------------------------------------------------------------
	// 2. SETUP 3 REAL TCP PROVIDER NODES WITH CAS STORES
	// -------------------------------------------------------------
	type providerNode struct {
		host        host.Host
		edPriv      ed25519.PrivateKey
		edPub       ed25519.PublicKey
		store       *storage.FSStorage
		storePath   string
		proofEngine *ProviderProofEngine
		handler     *AvailabilityStreamHandler
		vouchersMu  sync.Mutex
		vouchers    []payment.PaymentState
	}

	numProviders := 3
	providers := make([]*providerNode, numProviders)

	for i := 0; i < numProviders; i++ {
		pHost, pEdPriv, pEdPub := createRealTCPNode(t, ctx)
		storeDir := t.TempDir()
		pStore := storage.NewFSStore(storeDir)

		engine, err := NewProviderProofEngine(pHost.ID().String(), pEdPriv, pStore)
		if err != nil {
			t.Fatalf("failed to create provider proof engine %d: %v", i, err)
		}

		pNode := &providerNode{
			host:        pHost,
			edPriv:      pEdPriv,
			edPub:       pEdPub,
			store:       pStore,
			storePath:   storeDir,
			proofEngine: engine,
		}

		// Register stream handler for /cipher/availability/1.0.0
		pNode.handler = NewAvailabilityStreamHandler(pHost, engine, func(v payment.PaymentState) {
			pNode.vouchersMu.Lock()
			defer pNode.vouchersMu.Unlock()
			pNode.vouchers = append(pNode.vouchers, v)
		})

		// Connect Publisher -> Provider over TCP
		provAddrInfo := peer.AddrInfo{
			ID:    pHost.ID(),
			Addrs: pHost.Addrs(),
		}
		if err := pubHost.Connect(ctx, provAddrInfo); err != nil {
			t.Fatalf("publisher failed to dial provider %d over TCP: %v", i, err)
		}

		// Register Provider in Coordinator Bridge
		provEthPriv, _ := crypto.GenerateKey()
		provEthAddr := crypto.PubkeyToAddress(provEthPriv.PublicKey)
		if err := coordBridge.RegisterProvider(pHost.ID().String(), provEthAddr); err != nil {
			t.Fatalf("failed to register provider %d in bridge: %v", i, err)
		}

		providers[i] = pNode
		defer pHost.Close()
	}

	t.Logf("[✓] Publisher [%s] successfully connected over TCP to %d live Provider nodes",
		pubHost.ID(), numProviders)

	// -------------------------------------------------------------
	// 3. SEED TEST CONTENT AND REPLICATE CHUNKS
	// -------------------------------------------------------------
	testChunks := [][]byte{
		[]byte("cipher-protocol-block-zero-payload-data-alpha"),
		[]byte("cipher-protocol-block-one-payload-data-beta"),
		[]byte("cipher-protocol-block-two-payload-data-gamma"),
		[]byte("cipher-protocol-block-three-payload-data-delta"),
		[]byte("cipher-protocol-block-four-payload-data-epsilon"),
	}

	merkleTree, err := BuildMerkleTreeFromChunks(testChunks)
	if err != nil {
		t.Fatalf("failed to build merkle tree: %v", err)
	}
	fileMerkleRoot := merkleTree.Root()
	fileID := "file-content-net-test-999"

	// Replicate all chunks into all providers' proof engines & CAS storage
	for pIdx, pNode := range providers {
		_, err := pNode.proofEngine.RegisterFileChunks(fileID, testChunks)
		if err != nil {
			t.Fatalf("failed to register file chunks on provider %d: %v", pIdx, err)
		}
	}

	contractID := availabilitytypes.ContractID("avail-contract-net-test")
	var escrowAgreementID [32]byte
	rand.Read(escrowAgreementID[:])

	if err := coordBridge.RegisterContractSchedule(contractID, escrowAgreementID, 100, 1000); err != nil {
		t.Fatalf("failed to register contract schedule: %v", err)
	}

	// =========================================================================
	// SUBTEST 1: PARALLEL CONCURRENT CHALLENGES ACROSS MULTIPLE PROVIDERS
	// =========================================================================
	t.Run("ConcurrentMultiProviderChallenges", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make(chan error, numProviders*len(testChunks))

		for pIdx, pNode := range providers {
			for chunkIdx := 0; chunkIdx < len(testChunks); chunkIdx++ {
				wg.Add(1)
				go func(p *providerNode, pIndex int, cIdx int) {
					defer wg.Done()

					nonce := make([]byte, 32)
					rand.Read(nonce)

					challenge := availabilitytypes.Challenge{
						ChallengeID: availabilitytypes.ChallengeID(fmt.Sprintf("challenge-p%d-c%d", pIndex, cIdx)),
						ContractID:  contractID,
						ProviderID:  p.host.ID().String(),
						FileID:      fileID,
						ChunkID:     cIdx,
						Nonce:       nonce,
						CreatedAt:   time.Now().UTC(),
					}

					// 1. Stream Challenge Request over TCP
					resp, err := pubClient.ChallengeProvider(ctx, p.host.ID(), challenge)
					if err != nil {
						errs <- fmt.Errorf("provider %d chunk %d challenge failed: %w", pIndex, cIdx, err)
						return
					}

					// 2. Cryptographic Verification on Publisher
					valid, err := verification.VerifyChallengeResponse(
						challenge,
						resp,
						fileMerkleRoot,
						p.edPub,
						10*time.Second,
					)
					if err != nil {
						errs <- fmt.Errorf("provider %d chunk %d verification error: %w", pIndex, cIdx, err)
						return
					}
					if !valid {
						errs <- fmt.Errorf("provider %d chunk %d verification failed: invalid proof", pIndex, cIdx)
						return
					}
				}(pNode, pIdx, chunkIdx)
			}
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			t.Fatalf("concurrent challenge failure: %v", err)
		}
		t.Logf("[✓] Successfully executed and verified %d concurrent challenges across %d providers over TCP",
			numProviders*len(testChunks), numProviders)
	})

	// =========================================================================
	// SUBTEST 2: PROGRESSIVE OFF-CHAIN PAYMENT VOUCHER STREAMING
	// =========================================================================
	t.Run("PaymentVoucherStreamingAndReceipt", func(t *testing.T) {
		for pIdx, pNode := range providers {
			provContractID := availabilitytypes.ContractID(fmt.Sprintf("avail-contract-p%d", pIdx))
			var provEscrowID [32]byte
			rand.Read(provEscrowID[:])

			if err := coordBridge.RegisterContractSchedule(provContractID, provEscrowID, 100, 1000); err != nil {
				t.Fatalf("failed to register contract schedule for provider %d: %v", pIdx, err)
			}

			// Simulate 3 sequential challenge PASS rounds for each provider
			for round := 1; round <= 3; round++ {
				res := availabilitytypes.AvailabilityResult{
					ContractID:  provContractID,
					ProviderID:  pNode.host.ID().String(),
					Period:      uint64(round),
					ChallengeID: availabilitytypes.ChallengeID(fmt.Sprintf("seq-round-%d-p%d", round, pIdx)),
					Result:      availabilitytypes.AvailabilityPass,
					Timestamp:   time.Now().UTC(),
				}

				dispatchRes, err := coordBridge.DispatchAvailabilityResult(res)
				if err != nil {
					t.Fatalf("failed to dispatch availability result: %v", err)
				}
				if dispatchRes.Kind != coordinator.DispatchKindPass {
					t.Fatalf("expected DispatchKindPass, got %v", dispatchRes.Kind)
				}

				// Stream MsgPaymentVoucher (0x22) over TCP
				err = pubClient.SendPaymentVoucher(ctx, pNode.host.ID(), dispatchRes.PaymentState)
				if err != nil {
					t.Fatalf("failed to send payment voucher to provider %d round %d: %v", pIdx, round, err)
				}
			}

			// Allow short buffer for async handler receipt
			time.Sleep(50 * time.Millisecond)

			pNode.vouchersMu.Lock()
			vCount := len(pNode.vouchers)
			var latestCumulative uint64
			if vCount > 0 {
				latestCumulative = pNode.vouchers[vCount-1].CumulativePayment
			}
			pNode.vouchersMu.Unlock()

			if vCount != 3 {
				t.Fatalf("provider %d expected 3 vouchers, got %d", pIdx, vCount)
			}
			if latestCumulative != 300 { // 3 passes * 100 wei
				t.Fatalf("provider %d expected latest cumulative payment 300, got %d", pIdx, latestCumulative)
			}

			// Verify publisher's signature on latest voucher
			validSig := VerifyPaymentVoucher(pNode.vouchers[2], pubEdPub, crypto.PubkeyToAddress(pubEthPriv.PublicKey))
			if !validSig {
				t.Fatalf("voucher signature verification failed on provider %d", pIdx)
			}
		}
		t.Logf("[✓] Successfully streamed and verified progressive payment vouchers across all providers")
	})

	// =========================================================================
	// SUBTEST 3: ON-DEMAND DYNAMIC CONTENT INDEXING OVER WIRE
	// =========================================================================
	t.Run("DynamicOnDemandContentIndexingOverWire", func(t *testing.T) {
		targetProv := providers[0]

		// Ingest a brand new dynamic file directly into provider's CAS storage
		dynamicChunks := [][]byte{
			[]byte("dynamic-wire-chunk-zero"),
			[]byte("dynamic-wire-chunk-one"),
			[]byte("dynamic-wire-chunk-two"),
		}
		dynamicTree, err := BuildMerkleTreeFromChunks(dynamicChunks)
		if err != nil {
			t.Fatalf("failed to build dynamic merkle tree: %v", err)
		}
		dynamicRoot := dynamicTree.Root()

		// Store raw chunks in CAS
		var chunkIDs []core.ChunkID
		for i, cData := range dynamicChunks {
			var cid core.ChunkID
			cid[0] = 0xAA
			cid[1] = byte(i)
			_ = targetProv.store.PutChunk(ctx, &core.Chunk{
				Header: core.ChunkHeader{ID: cid},
				Data:   cData,
			})
			chunkIDs = append(chunkIDs, cid)
		}

		// Store Manifest in CAS
		var descID core.ContentID
		descID[0] = 0xDE
		descID[1] = 0xAD
		m := manifest.Manifest{
			Descriptor: manifest.ContentDescriptor{ID: descID, Type: manifest.TypeFile},
			ChunkIDs:   chunkIDs,
		}
		mBytes, _ := m.Serialize()
		_ = targetProv.store.PutManifestBytes(ctx, descID, mBytes)
		dynamicFileID := hex.EncodeToString(descID[:])

		// Publisher challenges for chunk 1 of this un-indexed file over TCP
		nonce := make([]byte, 32)
		rand.Read(nonce)
		challenge := availabilitytypes.Challenge{
			ChallengeID: "challenge-dynamic-001",
			ContractID:  contractID,
			ProviderID:  targetProv.host.ID().String(),
			FileID:      dynamicFileID,
			ChunkID:     1, // chunk 1
			Nonce:       nonce,
			CreatedAt:   time.Now().UTC(),
		}

		resp, err := pubClient.ChallengeProvider(ctx, targetProv.host.ID(), challenge)
		if err != nil {
			t.Fatalf("dynamic challenge request failed over TCP: %v", err)
		}

		valid, err := verification.VerifyChallengeResponse(
			challenge,
			resp,
			dynamicRoot,
			targetProv.edPub,
			10*time.Second,
		)
		if err != nil || !valid {
			t.Fatalf("dynamic challenge verification failed: %v", err)
		}
		t.Logf("[✓] Dynamic on-demand CAS chunk loading & Merkle proof generation succeeded over wire")
	})

	// =========================================================================
	// SUBTEST 4: ADVERSARIAL & NETWORK FAULT INJECTIONS OVER WIRE
	// =========================================================================
	t.Run("AdversarialAndFaultInjectionHandling", func(t *testing.T) {
		targetProv := providers[1]

		// 1. Unknown File ID -> Provider must return clean MsgChallengeError (0x23)
		nonce := make([]byte, 32)
		rand.Read(nonce)
		nonExistentChallenge := availabilitytypes.Challenge{
			ChallengeID: "challenge-unknown-file",
			ContractID:  contractID,
			ProviderID:  targetProv.host.ID().String(),
			FileID:      "non-existent-file-id-404",
			ChunkID:     0,
			Nonce:       nonce,
			CreatedAt:   time.Now().UTC(),
		}

		_, err = pubClient.ChallengeProvider(ctx, targetProv.host.ID(), nonExistentChallenge)
		if err == nil {
			t.Fatalf("expected error for non-existent file challenge, got nil")
		}
		t.Logf("[✓] Provider returned clean wire error for non-existent file: %v", err)

		// 2. Out-of-bounds Chunk ID -> Provider must return clean MsgChallengeError (0x23)
		oobChallenge := availabilitytypes.Challenge{
			ChallengeID: "challenge-oob-chunk",
			ContractID:  contractID,
			ProviderID:  targetProv.host.ID().String(),
			FileID:      fileID,
			ChunkID:     9999, // Out of bounds
			Nonce:       nonce,
			CreatedAt:   time.Now().UTC(),
		}
		_, err = pubClient.ChallengeProvider(ctx, targetProv.host.ID(), oobChallenge)
		if err == nil {
			t.Fatalf("expected error for out-of-bounds chunk, got nil")
		}
		t.Logf("[✓] Provider returned clean wire error for out-of-bounds chunk: %v", err)

		// 3. Stale Nonce Replay Detection
		validChallenge := availabilitytypes.Challenge{
			ChallengeID: "challenge-legit-001",
			ContractID:  contractID,
			ProviderID:  targetProv.host.ID().String(),
			FileID:      fileID,
			ChunkID:     2,
			Nonce:       []byte("original-nonce-alpha"),
			CreatedAt:   time.Now().UTC(),
		}
		resp, err := pubClient.ChallengeProvider(ctx, targetProv.host.ID(), validChallenge)
		if err != nil {
			t.Fatalf("legit challenge failed: %v", err)
		}

		// Replay attack: challenger checks with different nonce
		replayedChallenge := validChallenge
		replayedChallenge.Nonce = []byte("tampered-different-nonce")
		valid, _ := verification.VerifyChallengeResponse(
			replayedChallenge,
			resp,
			fileMerkleRoot,
			targetProv.edPub,
			10*time.Second,
		)
		if valid {
			t.Fatalf("adversarial replay attack succeeded when it should have been rejected!")
		}
		t.Logf("[✓] Anti-replay nonce protection successfully rejected stale proof digest")

		// 4. Forged Ed25519 Transport Key Detection
		fakeEdPub, _, _ := ed25519.GenerateKey(rand.Reader)
		valid, _ = verification.VerifyChallengeResponse(
			validChallenge,
			resp,
			fileMerkleRoot,
			fakeEdPub,
			10*time.Second,
		)
		if valid {
			t.Fatalf("verification accepted signature from unauthorized Ed25519 public key!")
		}
		t.Logf("[✓] Publisher successfully rejected proof signed with unauthenticated key")
	})

	// =========================================================================
	// SUBTEST 5: NETWORK TIMEOUT & CONNECTION DROP RESILIENCE
	// =========================================================================
	t.Run("NetworkTimeoutAndClosedStreamResilience", func(t *testing.T) {
		// Spawn temporary ephemeral node and close immediately
		ephemeralHost, _, _ := createRealTCPNode(t, ctx)
		ephemeralID := ephemeralHost.ID()
		_ = pubHost.Connect(ctx, peer.AddrInfo{ID: ephemeralID, Addrs: ephemeralHost.Addrs()})
		ephemeralHost.Close() // Simulate sudden drop/offline provider

		shortCtx, shortCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer shortCancel()

		challenge := availabilitytypes.Challenge{
			ChallengeID: "challenge-dropped-peer",
			ContractID:  contractID,
			ProviderID:  ephemeralID.String(),
			FileID:      fileID,
			ChunkID:     0,
			Nonce:       []byte("test-nonce"),
			CreatedAt:   time.Now().UTC(),
		}

		_, err := pubClient.ChallengeProvider(shortCtx, ephemeralID, challenge)
		if err == nil {
			t.Fatalf("expected connection error for dropped provider, got nil")
		}
		t.Logf("[✓] Publisher gracefully handled disconnected peer without deadlocking: %v", err)
	})
}
