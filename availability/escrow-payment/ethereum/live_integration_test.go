//go:build integration

package ethereum

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	availabilitytypes "cipher/availability/availability-contracts/types"
	bindings "cipher/availability/escrow-payment/bindings"
	coordinator "cipher/availability/escrow-payment/coordinator"
	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

const setupABI = `[
  {"inputs":[{"name":"provider","type":"address"},{"name":"fileID","type":"bytes32"},{"name":"reward","type":"uint256"},{"name":"periods","type":"uint64"},{"name":"duration","type":"uint64"},{"name":"rulesHash","type":"bytes32"},{"name":"collateralRequired","type":"uint256"}],"name":"createContract","outputs":[{"name":"contractID","type":"bytes32"}],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"contractID","type":"bytes32"}],"name":"fundEscrow","outputs":[],"stateMutability":"payable","type":"function"},
  {"inputs":[{"name":"contractID","type":"bytes32"}],"name":"activateContract","outputs":[],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"contractID","type":"bytes32"},{"name":"paymentState","components":[{"name":"contractID","type":"bytes32"},{"name":"publisher","type":"address"},{"name":"provider","type":"address"},{"name":"sequence","type":"uint64"},{"name":"period","type":"uint64"},{"name":"cumulativePayment","type":"uint256"},{"name":"lastChallengeID","type":"bytes32"},{"name":"validUntil","type":"uint64"},{"name":"status","type":"uint8"},{"name":"signature","type":"bytes"}],"type":"tuple"}],"name":"settleContract","outputs":[],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"contractID","type":"bytes32"}],"name":"getContractState","outputs":[{"name":"state","type":"uint8"}],"stateMutability":"view","type":"function"}
]`

// TestAvailabilityPaymentSettlementLive proves the actual boundary flow against
// a deployed local contract. It is intentionally opt-in because it needs a
// running RPC node and a deployment that survives for the test duration.
func TestAvailabilityPaymentSettlementLive(t *testing.T) {
	rpcURL, contractText := os.Getenv("CIPHER_ESCROW_RPC_URL"), os.Getenv("CIPHER_ESCROW_ADDRESS")
	if rpcURL == "" || contractText == "" {
		t.Skip("set CIPHER_ESCROW_RPC_URL and CIPHER_ESCROW_ADDRESS to run the local-chain integration test")
	}
	contractAddress := common.HexToAddress(contractText)
	if contractAddress == (common.Address{}) {
		t.Fatal("CIPHER_ESCROW_ADDRESS is invalid")
	}
	ctx := context.Background()
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		t.Fatalf("dial local RPC: %v", err)
	}
	defer client.Close()
	chainID, err := client.ChainID(ctx)
	if err != nil {
		t.Fatalf("read chain ID: %v", err)
	}
	publisherKey, err := crypto.HexToECDSA("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
	if err != nil {
		t.Fatalf("load Hardhat publisher key: %v", err)
	}
	providerKey, err := crypto.HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
	if err != nil {
		t.Fatalf("load Hardhat provider key: %v", err)
	}
	publisher, provider := crypto.PubkeyToAddress(publisherKey.PublicKey), crypto.PubkeyToAddress(providerKey.PublicKey)
	parsedABI, err := abi.JSON(strings.NewReader(setupABI))
	if err != nil {
		t.Fatalf("parse setup ABI: %v", err)
	}
	contract := bind.NewBoundContract(contractAddress, parsedABI, client, client, client)

	reward := big.NewInt(1_000_000_000_000_000)
	fileID, rulesHash := crypto.Keccak256Hash([]byte("live-file")), crypto.Keccak256Hash([]byte("live-rules"))
	var callResult []interface{}
	if err := contract.Call(&bind.CallOpts{Context: ctx, From: publisher}, &callResult, "createContract", provider, fileID, reward, uint64(1), uint64(3600), rulesHash, big.NewInt(0)); err != nil {
		t.Fatalf("simulate createContract: %v", err)
	}
	escrowID := *abi.ConvertType(callResult[0], new([32]byte)).(*[32]byte)
	transactAndWait(t, ctx, client, contract, publisherKey, chainID, nil, "createContract", provider, fileID, reward, uint64(1), uint64(3600), rulesHash, big.NewInt(0))
	transactAndWait(t, ctx, client, contract, publisherKey, chainID, reward, "fundEscrow", escrowID)
	transactAndWait(t, ctx, client, contract, publisherKey, chainID, nil, "activateContract", escrowID)

	signer, err := NewSigner(publisherKey, contractAddress, chainID)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	adapter, err := DialEscrowAdapter(ctx, rpcURL, contractAddress, publisherKey, chainID)
	if err != nil {
		t.Fatalf("DialEscrowAdapter: %v", err)
	}
	defer adapter.Close()
	workflow, err := coordinator.NewPublisherCoordinator(publisher.Hex(), signer, adapter)
	if err != nil {
		t.Fatalf("NewPublisherCoordinator: %v", err)
	}
	providerBinding, _ := bindings.ParseEthereumAddress(provider.Hex())
	if err := workflow.RegisterProvider("provider-peer", providerBinding); err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}
	escrowBinding, _ := bindings.ParseEscrowContractID(common.BytesToHash(escrowID[:]).Hex())
	availabilityID := availabilitytypes.ContractID("availability-live-contract")
	if err := workflow.RegisterContract(availabilityID, escrowBinding); err != nil {
		t.Fatalf("RegisterContract: %v", err)
	}
	state, err := workflow.CreateAndSignPaymentState(interfaces.AvailabilityResult{
		ContractID: availabilityID, ProviderID: "provider-peer", Period: 1,
		ChallengeID: availabilitytypes.ChallengeID("live-challenge-1"), Result: interfaces.AvailabilityPass,
	}, reward.Uint64(), 1)
	if err != nil {
		t.Fatalf("availability PASS through coordinator signing: %v", err)
	}

	if err := workflow.SubmitLatestPaymentStateOnChain(escrowBinding.String()); err != nil {
		t.Fatalf("submit latest payment state on chain: %v", err)
	}

	transactAndWait(t, ctx, client, contract, providerKey, chainID, nil, "settleContract", escrowID, toSolidityPaymentState(state))
	var stateResult []interface{}
	if err := contract.Call(&bind.CallOpts{Context: ctx}, &stateResult, "getContractState", escrowID); err != nil {
		t.Fatalf("read settlement state: %v", err)
	}
	if got := *abi.ConvertType(stateResult[0], new(uint8)).(*uint8); got != 4 {
		t.Fatalf("contract state after settlement = %d, want 4 (Settled)", got)
	}
}

func transactAndWait(t *testing.T, ctx context.Context, client *ethclient.Client, contract *bind.BoundContract, key *ecdsa.PrivateKey, chainID *big.Int, value *big.Int, method string, arguments ...interface{}) {
	t.Helper()
	auth, err := bind.NewKeyedTransactorWithChainID(key, chainID)
	if err != nil {
		t.Fatalf("create %s signer: %v", method, err)
	}
	auth.Context = ctx
	if value != nil {
		auth.Value = value
	}
	tx, err := contract.Transact(auth, method, arguments...)
	if err != nil {
		t.Fatalf("submit %s: %v", method, err)
	}
	receipt, err := bind.WaitMined(ctx, client, tx)
	if err != nil {
		t.Fatalf("wait for %s: %v", method, err)
	}
	if receipt.Status != 1 {
		t.Fatalf("%s reverted: %s", method, tx.Hash())
	}
}

func toSolidityPaymentState(state payment.PaymentState) solidityPaymentState {
	return solidityPaymentState{
		ContractID: common.HexToHash(state.ContractID), Publisher: common.HexToAddress(state.Publisher), Provider: common.HexToAddress(state.Provider),
		Sequence: state.Sequence, Period: state.Period, CumulativePayment: new(big.Int).SetUint64(state.CumulativePayment),
		LastChallengeID: crypto.Keccak256Hash([]byte(state.LastChallengeID)), ValidUntil: state.ValidUntil, Status: statusCode(string(state.Status)), Signature: state.PublisherSignature,
	}
}
