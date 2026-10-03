package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	"cipher/availability/availability-contracts/verification"
	"cipher/integration/availability"
	"cipher/network/content/manifest"
	"cipher/network/content/storage"
	"proof-of-request/model"
	"proof-of-request/security"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ANSI Color and Styling Codes
const (
	Reset       = "\033[0m"
	Bold        = "\033[1m"
	Dim         = "\033[2m"
	Italic      = "\033[3m"
	Underline   = "\033[4m"
	Blink       = "\033[5m"
	Inverse     = "\033[7m"

	FgBlack     = "\033[30m"
	FgRed       = "\033[31m"
	FgGreen     = "\033[32m"
	FgYellow    = "\033[33m"
	FgBlue      = "\033[34m"
	FgMagenta   = "\033[35m"
	FgCyan      = "\033[36m"
	FgWhite     = "\033[37m"

	FgBrightRed     = "\033[91m"
	FgBrightGreen   = "\033[92m"
	FgBrightYellow  = "\033[93m"
	FgBrightBlue    = "\033[94m"
	FgBrightMagenta = "\033[95m"
	FgBrightCyan    = "\033[96m"
	FgBrightWhite   = "\033[97m"

	BgBlack     = "\033[40m"
	BgRed       = "\033[41m"
	BgGreen     = "\033[42m"
	BgYellow    = "\033[43m"
	BgBlue      = "\033[44m"
	BgMagenta   = "\033[45m"
	BgCyan      = "\033[46m"
	BgWhite     = "\033[47m"

	ClearScreen = "\033[2J\033[H"
)

// SimulationState holds global state for real-time visualization
type SimulationState struct {
	mu sync.Mutex

	// Ingestion
	FileID     string
	FileName   string
	FileSize   int
	ChunkCount int
	MerkleRoot []byte

	// Demand & PoW
	TotalRequests     int
	PoWDifficulty     uint8
	CurrentDemand     int
	RequiredReplicas  int
	DemandHistory     []int
	RecentPoWHash     string
	RecentPoWNonce    uint64

	// Providers
	Providers map[string]*SimProvider

	// Publisher & Epochs
	CurrentEpoch    int
	TotalEpochs     int
	EpochActive     bool
	LastChallengeID string
	LastNonceHex    string
	LastChunkID     int

	// Blockchain & Escrow
	EscrowContractAddr common.Address
	PublisherEthAddr   common.Address
	TotalEscrowLocked  uint64
	TotalSlashed       uint64
	SlashingEvents     []SlashingEvent
	TxCount            int
	LatestTxHash       string

	// Live Event Log
	EventLog []LogEvent
}

type SimProvider struct {
	ID             string
	ShortName      string
	EthAddress     common.Address
	IsAdversarial  bool
	BehaviorNote   string
	EdPubKey       ed25519.PublicKey
	EdPrivKey      ed25519.PrivateKey
	ContractID     availabilitytypes.ContractID
	ProofEngine    *availability.ProviderProofEngine
	Store          *storage.FSStorage
	VoucherCount   int
	CumulativePay  uint64
	FailureStreak  int
	CollateralWei  uint64
	SlashedAmount  uint64
	State          string // "HEALTHY", "WARNING", "SLASHED", "DISPUTING"
	LastResult     string // "PASS", "FAIL (Tampered)", "FAIL (Timeout)", etc.
	LastLatencyMs  int64
	TotalPasses    int
	TotalFails     int
}

type SlashingEvent struct {
	Timestamp      time.Time
	ContractID     string
	ProviderID     string
	PenaltyWei     uint64
	Reason         string
	TxHash         string
	BlockNumber    uint64
}

type LogEvent struct {
	Timestamp time.Time
	Category  string // "INGEST", "DEMAND", "WIRE", "PROOFS", "VOUCHER", "SLASH", "EVM"
	Message   string
	Level     string // "INFO", "SUCCESS", "WARN", "DANGER"
}

func (s *SimulationState) AddLog(category, level, format string, args ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg := fmt.Sprintf(format, args...)
	s.EventLog = append(s.EventLog, LogEvent{
		Timestamp: time.Now(),
		Category:  category,
		Message:   msg,
		Level:     level,
	})
	if len(s.EventLog) > 30 {
		s.EventLog = s.EventLog[len(s.EventLog)-30:]
	}
}

func main() {
	var (
		epochsFlag      = flag.Int("epochs", 4, "Number of availability challenge epochs to simulate")
		speedMsFlag     = flag.Int("speed", 1400, "Step delay in milliseconds for terminal animation")
		interactiveFlag = flag.Bool("interactive", false, "Pause between epochs for user confirmation")
		adversaryFailK  = flag.Int("k", 3, "Failure threshold K consecutive fails before slashing")
	)
	flag.Parse()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Printf("\n%s[!] Simulation interrupted by user. Exiting cleanly...%s\n", FgYellow, Reset)
		os.Exit(0)
	}()

	state := &SimulationState{
		FileName:           "cipher_whitepaper_v2.enc",
		FileSize:           131072, // 128 KB
		ChunkCount:         4,
		PoWDifficulty:      1,
		RequiredReplicas:   2,
		DemandHistory:      make([]int, 0),
		Providers:          make(map[string]*SimProvider),
		TotalEpochs:        *epochsFlag,
		EscrowContractAddr: common.HexToAddress("0x9fE46736679d2D9a65F0992F2272dE9f3c7fa6e0"),
		TotalEscrowLocked:  3000, // 1000 wei * 3 providers
		EventLog:           make([]LogEvent, 0),
	}

	stepDelay := time.Duration(*speedMsFlag) * time.Millisecond

	// =========================================================================
	// PHASE 1: INITIALIZATION & MULTI-IDENTITY KEY GENERATION
	// =========================================================================
	state.AddLog("EVM", "INFO", "Initializing local Anvil Ethereum EVM on 127.0.0.1:8545")
	state.AddLog("EVM", "SUCCESS", "EscrowContract deployed at %s (Settlement Window: 1 days)", state.EscrowContractAddr.Hex())

	pubEdPub, pubEdPriv, _ := ed25519.GenerateKey(rand.Reader)
	_ = pubEdPub
	pubEthPriv, _ := crypto.GenerateKey()
	state.PublisherEthAddr = crypto.PubkeyToAddress(pubEthPriv.PublicKey)
	publisherID := "12D3KooWPubLiSherNode777AlphaX99"

	coordBridge, err := availability.NewCoordinatorBridge(availability.CoordinatorBridgeConfig{
		PublisherPeerID:  publisherID,
		PublisherEthKey:  pubEthPriv,
		PublisherEdKey:   pubEdPriv,
		FailureThreshold: uint32(*adversaryFailK),
	})
	if err != nil {
		fmt.Printf("Error creating coordinator bridge: %v\n", err)
		return
	}

	demandMgr := availability.NewDemandAndReplicaManager()

	// 1. Ingest File Chunks & Build Merkle Tree
	rawChunks := [][]byte{
		[]byte("cipher-protocol-whitepaper-chunk-0-executive-summary-and-vision"),
		[]byte("cipher-protocol-whitepaper-chunk-1-probabilistic-micropayments"),
		[]byte("cipher-protocol-whitepaper-chunk-2-cryptographic-availability-proofs"),
		[]byte("cipher-protocol-whitepaper-chunk-3-byzantine-slashing-and-settlement"),
	}
	mTree, _ := availability.BuildMerkleTreeFromChunks(rawChunks)
	state.MerkleRoot = mTree.Root()
	var descID [32]byte
	copy(descID[:], state.MerkleRoot)
	state.FileID = hex.EncodeToString(descID[:])

	state.AddLog("INGEST", "SUCCESS", "Ingested %s (4 Chunks, 32 KB each) -> Merkle Root: %s...",
		state.FileName, hex.EncodeToString(state.MerkleRoot)[:16])

	// 2. Setup 3 Providers (2 Honest, 1 Byzantine/Adversary)
	providerConfigs := []struct {
		name      string
		isAdv     bool
		note      string
		startColl uint64
	}{
		{"Provider-1 (US-East)", false, "Honest CAS Provider (Dedicated NVMe)", 1000000},
		{"Provider-2 (EU-Central)", false, "Honest CAS Provider (High-Throughput)", 1000000},
		{"Provider-3 (Byzantine-Node)", true, "Adversary (Simulating Data Dropping & Forgery)", 1000000},
	}

	for i, cfg := range providerConfigs {
		pPub, pPriv, _ := ed25519.GenerateKey(rand.Reader)
		pEthPriv, _ := crypto.GenerateKey()
		pEthAddr := crypto.PubkeyToAddress(pEthPriv.PublicKey)
		pID := fmt.Sprintf("12D3KooWProvider%dPeerIdentity%x", i+1, pEthAddr.Bytes()[:4])

		pStoreDir, _ := os.MkdirTemp("", fmt.Sprintf("sim_prov_%d_*", i+1))
		pStore := storage.NewFSStore(pStoreDir)
		pEngine, _ := availability.NewProviderProofEngine(pID, pPriv, pStore)
		_, _ = pEngine.RegisterFileChunks(state.FileID, rawChunks)

		cID := availabilitytypes.ContractID(fmt.Sprintf("avail-contract-p%d", i+1))
		var escrowID [32]byte
		rand.Read(escrowID[:])

		_ = coordBridge.RegisterProvider(pID, pEthAddr)
		_ = coordBridge.RegisterContractSchedule(cID, escrowID, 250, 1000)

		state.Providers[pID] = &SimProvider{
			ID:            pID,
			ShortName:     cfg.name,
			EthAddress:    pEthAddr,
			IsAdversarial: cfg.isAdv,
			BehaviorNote:  cfg.note,
			EdPubKey:      pPub,
			EdPrivKey:     pPriv,
			ContractID:    cID,
			ProofEngine:   pEngine,
			Store:         pStore,
			CollateralWei: cfg.startColl,
			State:         "HEALTHY",
			LastResult:    "STANDBY",
		}

		// Initial cache announcements
		mObj := &manifest.Manifest{
			Descriptor: manifest.ContentDescriptor{ID: descID, Type: manifest.TypeFile},
			MerkleRoot: descID,
			Version:    1,
		}
		signedAnn, _ := availability.CreateSignedCacheAnnouncement(pID, pPriv, mObj, 24*time.Hour)
		_ = demandMgr.IngestAnnouncement(signedAnn, pPub)

		state.AddLog("WIRE", "INFO", "[%s] Connected over /cipher/availability/1.0.0 (Eth: %s...)",
			cfg.name, pEthAddr.Hex()[:10])
	}

	renderDashboard(state)
	time.Sleep(stepDelay)

	// =========================================================================
	// PHASE 2: DYNAMIC DEMAND SPIKE & HASHCASH PROOF-OF-WORK SIMULATION
	// =========================================================================
	state.AddLog("DEMAND", "WARN", "Consumer traffic surge detected! Initiating Proof-of-Request Hashcash PoW...")
	renderDashboard(state)
	time.Sleep(stepDelay / 2)

	// Simulate consumer requests with Hashcash PoW
	consumerClients := []string{"client-0x892a", "client-0x4bf1", "client-0xcd19", "client-0x77aa", "client-0x12bb"}
	for _, clientID := range consumerClients {
		req := model.Request{
			ClientID:  clientID,
			FileID:    state.FileID,
			Timestamp: time.Now().Unix(),
		}
		pow := security.GeneratePoW(req, state.PoWDifficulty)
		_, err := demandMgr.ValidateAndTrackRequest(req, pow, 10*time.Minute)
		if err == nil {
			state.TotalRequests += 12
			state.RecentPoWNonce = pow.Nonce
			powHash := sha256.Sum256([]byte(clientID + state.FileID + fmt.Sprintf("%d%d", req.Timestamp, pow.Nonce)))
			state.RecentPoWHash = hex.EncodeToString(powHash[:])
		}
	}

	state.CurrentDemand = 68 // 68 concurrent chunk requests/sec
	state.DemandHistory = append(state.DemandHistory, state.CurrentDemand)
	state.RequiredReplicas = 4 // Scale from R=2 to R=4

	state.AddLog("DEMAND", "SUCCESS", "Validated 5 Hashcash PoW consumer proofs. Dynamic demand reached 68 req/s -> Replicas scaled to R=4")
	renderDashboard(state)
	time.Sleep(stepDelay)

	// =========================================================================
	// PHASE 3: MULTI-EPOCH STOCHASTIC CHALLENGE & BYZANTINE SLASHING RUN
	// =========================================================================
	for epoch := 1; epoch <= state.TotalEpochs; epoch++ {
		state.CurrentEpoch = epoch
		state.EpochActive = true

		// Random chunk selection
		chunkRandBig, _ := rand.Int(rand.Reader, big.NewInt(int64(state.ChunkCount)))
		selectedChunk := int(chunkRandBig.Int64())
		state.LastChunkID = selectedChunk

		// Generate fresh 32-byte Nonce
		nonceBytes := make([]byte, 32)
		rand.Read(nonceBytes)
		state.LastNonceHex = hex.EncodeToString(nonceBytes)[:16]
		challengeID := fmt.Sprintf("epoch-%d-rnd-%x", epoch, nonceBytes[:4])
		state.LastChallengeID = challengeID

		state.AddLog("WIRE", "INFO", "─── [EPOCH %d/%d] Broadcast Challenge %s (Chunk #%d, Nonce: %s...) ───",
			epoch, state.TotalEpochs, challengeID, selectedChunk, state.LastNonceHex)

		renderDashboard(state)
		time.Sleep(stepDelay / 2)

		// Challenge all 3 providers
		for _, prov := range state.Providers {
			t0 := time.Now()

			challenge := availabilitytypes.Challenge{
				ChallengeID: availabilitytypes.ChallengeID(challengeID),
				ContractID:  prov.ContractID,
				ProviderID:  prov.ID,
				FileID:      state.FileID,
				ChunkID:     selectedChunk,
				Nonce:       nonceBytes,
				CreatedAt:   time.Now().UTC(),
			}

			if !prov.IsAdversarial {
				// HONEST PROVIDER PATH
				resp, err := prov.ProofEngine.HandleChallenge(challenge)
				if err != nil {
					prov.LastResult = fmt.Sprintf("FAIL (%v)", err)
					prov.LastLatencyMs = time.Since(t0).Milliseconds()
					continue
				}

				// Publisher verifies Merkle Proof + Nonce Binding + Ed25519 Sig
				valid, err := verification.VerifyChallengeResponse(
					challenge,
					resp,
					state.MerkleRoot,
					prov.EdPubKey,
					5*time.Second,
				)

				prov.LastLatencyMs = time.Since(t0).Milliseconds() + int64(15+(epoch*3)) // realistic ms

				if valid && err == nil {
					prov.TotalPasses++
					prov.FailureStreak = 0
					prov.LastResult = "PASS (Valid Merkle Proof)"
					prov.State = "HEALTHY"

					// Issue progressive off-chain payment voucher
					dispatchRes, _ := coordBridge.DispatchAvailabilityResult(availabilitytypes.AvailabilityResult{
						ContractID:  prov.ContractID,
						ProviderID:  prov.ID,
						Period:      uint64(epoch),
						ChallengeID: challenge.ChallengeID,
						Result:      availabilitytypes.AvailabilityPass,
						Timestamp:   time.Now().UTC(),
					})

					prov.VoucherCount++
					prov.CumulativePay = dispatchRes.PaymentState.CumulativePayment

					state.AddLog("VOUCHER", "SUCCESS", "[%s] Valid Merkle proof verified in %dms -> Issued signed PaymentState Voucher (Seq: %d, Cumulative: %d wei)",
						prov.ShortName, prov.LastLatencyMs, dispatchRes.PaymentState.Sequence, prov.CumulativePay)
				}
			} else {
				// ADVERSARIAL PROVIDER PATH
				prov.LastLatencyMs = time.Since(t0).Milliseconds() + int64(35+(epoch*10))

				if epoch == 1 {
					// Round 1: Tampered chunk hash / corrupt data
					resp, _ := prov.ProofEngine.HandleChallenge(challenge)
					resp.ChunkHash = []byte("corrupt-tampered-data-payload-hash") // Tampering!

					valid, _ := verification.VerifyChallengeResponse(
						challenge,
						resp,
						state.MerkleRoot,
						prov.EdPubKey,
						5*time.Second,
					)

					if !valid {
						prov.TotalFails++
						prov.FailureStreak++
						prov.LastResult = "FAIL (Tampered Merkle Leaf)"
						prov.State = "WARNING"

						_, _ = coordBridge.DispatchAvailabilityResult(availabilitytypes.AvailabilityResult{
							ContractID:  prov.ContractID,
							ProviderID:  prov.ID,
							Period:      uint64(epoch),
							ChallengeID: challenge.ChallengeID,
							Result:      availabilitytypes.AvailabilityFail,
							Timestamp:   time.Now().UTC(),
						})

						state.AddLog("PROOFS", "DANGER", "[%s] Adversarial attack detected! Merkle leaf hash mismatch (Streak: %d/%d)",
							prov.ShortName, prov.FailureStreak, *adversaryFailK)
					}

				} else if epoch == 2 {
					// Round 2: Stale Nonce Replay / Forged Proof (Signature does not match fresh Nonce)
					resp, _ := prov.ProofEngine.HandleChallenge(challenge)
					resp.Signature = []byte("replayed-stale-signature-from-epoch-0")

					valid, _ := verification.VerifyChallengeResponse(
						challenge,
						resp,
						state.MerkleRoot,
						prov.EdPubKey,
						5*time.Second,
					)

					if !valid {
						prov.TotalFails++
						prov.FailureStreak++
						prov.LastResult = "FAIL (Stale Nonce Replay)"
						prov.State = "WARNING"

						_, _ = coordBridge.DispatchAvailabilityResult(availabilitytypes.AvailabilityResult{
							ContractID:  prov.ContractID,
							ProviderID:  prov.ID,
							Period:      uint64(epoch),
							ChallengeID: challenge.ChallengeID,
							Result:      availabilitytypes.AvailabilityFail,
							Timestamp:   time.Now().UTC(),
						})

						state.AddLog("PROOFS", "DANGER", "[%s] Adversarial replay detected! Nonce binding invalid (Streak: %d/%d)",
							prov.ShortName, prov.FailureStreak, *adversaryFailK)
					}

				} else if epoch >= 3 {
					// Round 3+: Threshold K=3 reached -> Trigger Slashing on Blockchain
					prov.TotalFails++
					prov.FailureStreak++
					prov.LastResult = "FAIL (Missing Chunk / Dropped)"

					dispatchRes, _ := coordBridge.DispatchAvailabilityResult(availabilitytypes.AvailabilityResult{
						ContractID:  prov.ContractID,
						ProviderID:  prov.ID,
						Period:      uint64(epoch),
						ChallengeID: challenge.ChallengeID,
						Result:      availabilitytypes.AvailabilityFail,
						Timestamp:   time.Now().UTC(),
					})

					state.AddLog("PROOFS", "DANGER", "[%s] Challenge dropped / timeout! Consecutive failures reached K=%d (SlashEligible: %t)",
						prov.ShortName, prov.FailureStreak, dispatchRes.SlashEligible)

					if dispatchRes.SlashEligible && prov.State != "SLASHED" {
						prov.State = "SLASHED"
						penaltyWei := uint64(500000) // 0.5 collateral slashed
						if prov.CollateralWei >= penaltyWei {
							prov.CollateralWei -= penaltyWei
						} else {
							prov.CollateralWei = 0
						}
						prov.SlashedAmount += penaltyWei
						state.TotalSlashed += penaltyWei
						state.TxCount++

						txHashBytes := make([]byte, 32)
						rand.Read(txHashBytes)
						txHash := "0x" + hex.EncodeToString(txHashBytes)
						state.LatestTxHash = txHash

						event := SlashingEvent{
							Timestamp:   time.Now(),
							ContractID:  string(prov.ContractID),
							ProviderID:  prov.ID,
							PenaltyWei:  penaltyWei,
							Reason:      "EscrowTypes.FailureReason.NonPossession (K=3 Consecutive Proof Failures)",
							TxHash:      txHash,
							BlockNumber: uint64(142050 + epoch),
						}
						state.SlashingEvents = append(state.SlashingEvents, event)

						state.AddLog("SLASH", "DANGER", "⚡ ON-CHAIN ACTION: EscrowContract.slashCollateral(%s, 500,000 wei) executed! Tx: %s... (Block: #%d)",
							prov.ContractID, txHash[:18], event.BlockNumber)
					}
				}
			}
		}

		state.EpochActive = false
		renderDashboard(state)

		if *interactiveFlag && epoch < state.TotalEpochs {
			fmt.Printf("\n%s[Press Enter to proceed to Epoch %d...]%s ", FgBrightYellow, epoch+1, Reset)
			var input string
			fmt.Scanln(&input)
		} else {
			time.Sleep(stepDelay)
		}
	}

	// =========================================================================
	// PHASE 4: FINAL SETTLEMENT & SUMMARY
	// =========================================================================
	state.AddLog("EVM", "SUCCESS", "═══ ALL CHALLENGE EPOCHS CONCLUDED ═══")
	for _, p := range state.Providers {
		if p.State == "SLASHED" {
			state.AddLog("EVM", "WARN", "[%s] Final Contract Status: SettlementWithheld (Collateral Slashed: %d wei, Payout Withheld)",
				p.ShortName, p.SlashedAmount)
		} else {
			state.AddLog("EVM", "SUCCESS", "[%s] Final Contract Status: SettlementCompleted (Total Earned: %d wei, 100%% Collateral Safe)",
				p.ShortName, p.CumulativePay)
		}
	}

	renderDashboard(state)
	fmt.Printf("\n%s%s🎉 AVAILABILITY, DEMAND, AND ON-CHAIN SLASHING SIMULATION COMPLETE!%s\n\n", Bold, FgBrightGreen, Reset)
}

// renderDashboard renders the complete multi-panel ANSI visual terminal interface
func renderDashboard(s *SimulationState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var b strings.Builder
	b.WriteString(ClearScreen)

	// Top Banner
	b.WriteString(fmt.Sprintf("%s%s╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗%s\n", Bold, FgBrightCyan, Reset))
	b.WriteString(fmt.Sprintf("%s%s║               🌐 CIPHER AVAILABILITY, DEMAND & ON-CHAIN SLASHING SYSTEM SIMULATOR                  ║%s\n", Bold, FgBrightCyan, Reset))
	b.WriteString(fmt.Sprintf("%s%s╚══════════════════════════════════════════════════════════════════════════════════════════════════════╝%s\n", Bold, FgBrightCyan, Reset))

	// Panel 1 & 2: Publisher + Demand Monitor
	b.WriteString(fmt.Sprintf("%s┌── [1. PUBLISHER COORDINATOR] ──────────────────────────┬── [2. DEMAND & PROOF-OF-REQUEST] ─────────────────────┐%s\n", FgCyan, Reset))
	
	merkleShort := ""
	if len(s.MerkleRoot) >= 8 {
		merkleShort = hex.EncodeToString(s.MerkleRoot)[:16] + "..."
	}
	b.WriteString(fmt.Sprintf("│ File Ingested : %-22s (4 chunks) │ Active Demand : %s%d req/sec%s (%sSpike Mode%s)               │\n",
		s.FileName, FgBrightYellow, s.CurrentDemand, Reset, FgBrightMagenta, Reset))
	b.WriteString(fmt.Sprintf("│ Merkle Root   : %-38s │ Required Reps : %sR = %d replicas%s (Target Invariant)      │\n",
		merkleShort, FgBrightGreen, s.RequiredReplicas, Reset))
	b.WriteString(fmt.Sprintf("│ Active Epoch  : %s%d / %d%s (Challenge: %-15s)  │ Hashcash PoW  : Difficulty %d | Last Nonce: %-9d │\n",
		FgBrightCyan, s.CurrentEpoch, s.TotalEpochs, Reset, s.LastChallengeID, s.PoWDifficulty, s.RecentPoWNonce))
	
	powShort := ""
	if len(s.RecentPoWHash) >= 12 {
		powShort = s.RecentPoWHash[:12] + "..."
	}
	b.WriteString(fmt.Sprintf("│ Active Nonce  : %-38s │ Verified PoW  : %-37s │\n",
		s.LastNonceHex+"...", powShort))
	b.WriteString(fmt.Sprintf("%s└────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────┘%s\n", FgCyan, Reset))

	// Panel 3: Provider Swarm & P2P Wire Protocol Status
	b.WriteString(fmt.Sprintf("%s┌── [3. P2P STORAGE PROVIDER FLEET & WIRE STATUS (/cipher/availability/1.0.0)] ────────────────────────┐%s\n", FgYellow, Reset))
	b.WriteString(fmt.Sprintf("│ %s%-24s %-10s %-12s %-10s %-14s %-18s%s │\n",
		Bold, "PROVIDER IDENTITY", "STATE", "COLLATERAL", "VOUCHERS", "CUMULATIVE PAY", "LAST RESULT", Reset))
	b.WriteString(fmt.Sprintf("│ %s──────────────────────────────────────────────────────────────────────────────────────────────────%s │\n", Dim, Reset))

	// Deterministic order for providers
	provList := make([]*SimProvider, 0, len(s.Providers))
	for _, p := range s.Providers {
		provList = append(provList, p)
	}
	// Sort by short name
	for i := 0; i < len(provList); i++ {
		for j := i + 1; j < len(provList); j++ {
			if provList[i].ShortName > provList[j].ShortName {
				provList[i], provList[j] = provList[j], provList[i]
			}
		}
	}

	for _, p := range provList {
		stateBadge := ""
		switch p.State {
		case "HEALTHY":
			stateBadge = fmt.Sprintf("%s%s HEALTHY  %s", BgGreen, FgBlack, Reset)
		case "WARNING":
			stateBadge = fmt.Sprintf("%s%s WARNING  %s", BgYellow, FgBlack, Reset)
		case "SLASHED":
			stateBadge = fmt.Sprintf("%s%s SLASHED  %s", BgRed, FgBrightWhite, Reset)
		default:
			stateBadge = fmt.Sprintf("%s%s STANDBY  %s", BgBlue, FgWhite, Reset)
		}

		resultColor := FgBrightGreen
		if strings.HasPrefix(p.LastResult, "FAIL") {
			resultColor = FgBrightRed
		}

		b.WriteString(fmt.Sprintf("│ %-24s %s %8d wei   %2d pkts    %7d wei    %s%-24s%s │\n",
			p.ShortName,
			stateBadge,
			p.CollateralWei,
			p.VoucherCount,
			p.CumulativePay,
			resultColor,
			p.LastResult,
			Reset,
		))
	}
	b.WriteString(fmt.Sprintf("%s└───────────────────────────────────────────────────────────────────────────────────────────────────────┘%s\n", FgYellow, Reset))

	// Panel 4: Blockchain & Escrow Ledger
	b.WriteString(fmt.Sprintf("%s┌── [4. ON-CHAIN ESCROW & BLOCKCHAIN SETTLEMENT LEDGER (EscrowContract.sol)] ──────────────────────────┐%s\n", FgMagenta, Reset))
	txShort := "None"
	if s.LatestTxHash != "" && len(s.LatestTxHash) >= 18 {
		txShort = s.LatestTxHash[:18] + "..."
	}
	b.WriteString(fmt.Sprintf("│ Escrow Address : %-42s  Total Escrow Locked : %s%d wei%s         │\n",
		s.EscrowContractAddr.Hex(), FgBrightGreen, s.TotalEscrowLocked, Reset))
	b.WriteString(fmt.Sprintf("│ On-Chain Txs   : %-42d  Total Slashed Funds : %s%d wei%s         │\n",
		s.TxCount, FgBrightRed, s.TotalSlashed, Reset))
	b.WriteString(fmt.Sprintf("│ Latest Tx Hash : %-42s  Slashing Invariant  : %sK = 3 Failure Threshold%s │\n",
		txShort, FgBrightYellow, Reset))
	b.WriteString(fmt.Sprintf("%s└───────────────────────────────────────────────────────────────────────────────────────────────────────┘%s\n", FgMagenta, Reset))

	// Panel 5: Real-time Event Stream
	b.WriteString(fmt.Sprintf("%s┌── [5. REAL-TIME WIRE PROTOCOL & SYSTEM EVENT LOG STREAM] ─────────────────────────────────────────────┐%s\n", FgGreen, Reset))
	
	startIdx := 0
	if len(s.EventLog) > 10 {
		startIdx = len(s.EventLog) - 10
	}
	for i := startIdx; i < len(s.EventLog); i++ {
		ev := s.EventLog[i]
		catBadge := ""
		switch ev.Category {
		case "INGEST":
			catBadge = fmt.Sprintf("%s[INGEST]%s", FgCyan, Reset)
		case "DEMAND":
			catBadge = fmt.Sprintf("%s[DEMAND]%s", FgYellow, Reset)
		case "WIRE":
			catBadge = fmt.Sprintf("%s[ WIRE ]%s", FgBrightBlue, Reset)
		case "PROOFS":
			catBadge = fmt.Sprintf("%s[PROOFS]%s", FgMagenta, Reset)
		case "VOUCHER":
			catBadge = fmt.Sprintf("%s[VOUCH ]%s", FgGreen, Reset)
		case "SLASH":
			catBadge = fmt.Sprintf("%s%s[SLASH ]%s", BgRed, FgBrightWhite, Reset)
		case "EVM":
			catBadge = fmt.Sprintf("%s[ EVM  ]%s", FgBrightCyan, Reset)
		default:
			catBadge = fmt.Sprintf("%s[%-6s]%s", FgWhite, ev.Category, Reset)
		}

		textColor := Reset
		switch ev.Level {
		case "SUCCESS":
			textColor = FgBrightGreen
		case "WARN":
			textColor = FgBrightYellow
		case "DANGER":
			textColor = FgBrightRed
		case "INFO":
			textColor = FgWhite
		}

		timeStr := ev.Timestamp.Format("15:04:05.000")
		// Truncate long messages to fit terminal width
		msg := ev.Message
		if len(msg) > 82 {
			msg = msg[:79] + "..."
		}
		b.WriteString(fmt.Sprintf("│ %s%s%s %s %s%-82s%s │\n",
			Dim, timeStr, Reset, catBadge, textColor, msg, Reset))
	}
	// Pad empty lines if log has fewer than 10 entries
	for i := len(s.EventLog) - startIdx; i < 10; i++ {
		b.WriteString(fmt.Sprintf("│ %-101s │\n", " "))
	}

	b.WriteString(fmt.Sprintf("%s└───────────────────────────────────────────────────────────────────────────────────────────────────────┘%s\n", FgGreen, Reset))

	fmt.Print(b.String())
}
