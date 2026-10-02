package ethereum

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

// escrowABI is the complete ABI for EscrowContract.sol, covering contract
// creation, escrow funding, collateral deposit, contract activation,
// optimistic payment submission, final settlement, dispute/slashing, and queries.
const escrowABI = `[
  {
    "inputs": [
      {"internalType": "address", "name": "provider", "type": "address"},
      {"internalType": "bytes32", "name": "fileID", "type": "bytes32"},
      {"internalType": "uint256", "name": "reward", "type": "uint256"},
      {"internalType": "uint64", "name": "periods", "type": "uint64"},
      {"internalType": "uint64", "name": "duration", "type": "uint64"},
      {"internalType": "bytes32", "name": "rulesHash", "type": "bytes32"},
      {"internalType": "uint256", "name": "collateralRequired", "type": "uint256"}
    ],
    "name": "createContract",
    "outputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "fundEscrow",
    "outputs": [],
    "stateMutability": "payable",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "depositCollateral",
    "outputs": [],
    "stateMutability": "payable",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "activateContract",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {
        "components": [
          {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
          {"internalType": "address", "name": "publisher", "type": "address"},
          {"internalType": "address", "name": "provider", "type": "address"},
          {"internalType": "uint64", "name": "sequence", "type": "uint64"},
          {"internalType": "uint64", "name": "period", "type": "uint64"},
          {"internalType": "uint256", "name": "cumulativePayment", "type": "uint256"},
          {"internalType": "bytes32", "name": "lastChallengeID", "type": "bytes32"},
          {"internalType": "uint64", "name": "validUntil", "type": "uint64"},
          {"internalType": "uint8", "name": "status", "type": "uint8"},
          {"internalType": "bytes", "name": "signature", "type": "bytes"}
        ],
        "internalType": "struct EscrowTypes.PaymentState",
        "name": "paymentState",
        "type": "tuple"
      }
    ],
    "name": "submitPaymentState",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {
        "components": [
          {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
          {"internalType": "address", "name": "publisher", "type": "address"},
          {"internalType": "address", "name": "provider", "type": "address"},
          {"internalType": "uint64", "name": "sequence", "type": "uint64"},
          {"internalType": "uint64", "name": "period", "type": "uint64"},
          {"internalType": "uint256", "name": "cumulativePayment", "type": "uint256"},
          {"internalType": "bytes32", "name": "lastChallengeID", "type": "bytes32"},
          {"internalType": "uint64", "name": "validUntil", "type": "uint64"},
          {"internalType": "uint8", "name": "status", "type": "uint8"},
          {"internalType": "bytes", "name": "signature", "type": "bytes"}
        ],
        "internalType": "struct EscrowTypes.PaymentState",
        "name": "paymentState",
        "type": "tuple"
      }
    ],
    "name": "settleContract",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "refundPublisher",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "returnCollateral",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "terminateExpiredContract",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"internalType": "enum EscrowTypes.FailureReason", "name": "reason", "type": "uint8"}
    ],
    "name": "markFailure",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {
        "components": [
          {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
          {"internalType": "address", "name": "publisher", "type": "address"},
          {"internalType": "address", "name": "provider", "type": "address"},
          {"internalType": "uint64", "name": "sequence", "type": "uint64"},
          {"internalType": "uint64", "name": "period", "type": "uint64"},
          {"internalType": "uint256", "name": "cumulativePayment", "type": "uint256"},
          {"internalType": "bytes32", "name": "lastChallengeID", "type": "bytes32"},
          {"internalType": "uint64", "name": "validUntil", "type": "uint64"},
          {"internalType": "uint8", "name": "status", "type": "uint8"},
          {"internalType": "bytes", "name": "signature", "type": "bytes"}
        ],
        "internalType": "struct EscrowTypes.PaymentState",
        "name": "paymentState",
        "type": "tuple"
      }
    ],
    "name": "disputeFailure",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"internalType": "uint256", "name": "penalty", "type": "uint256"}
    ],
    "name": "slashCollateral",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "getContractState",
    "outputs": [{"internalType": "enum EscrowTypes.ContractState", "name": "", "type": "uint8"}],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "getPaymentState",
    "outputs": [
      {
        "components": [
          {"internalType": "bytes32", "name": "contractID", "type": "bytes32"},
          {"internalType": "address", "name": "publisher", "type": "address"},
          {"internalType": "address", "name": "provider", "type": "address"},
          {"internalType": "uint64", "name": "sequence", "type": "uint64"},
          {"internalType": "uint64", "name": "period", "type": "uint64"},
          {"internalType": "uint256", "name": "cumulativePayment", "type": "uint256"},
          {"internalType": "bytes32", "name": "lastChallengeID", "type": "bytes32"},
          {"internalType": "uint64", "name": "validUntil", "type": "uint64"},
          {"internalType": "uint8", "name": "status", "type": "uint8"},
          {"internalType": "bytes", "name": "signature", "type": "bytes"}
        ],
        "internalType": "struct EscrowTypes.PaymentState",
        "name": "",
        "type": "tuple"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "getEscrowBalance",
    "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [{"internalType": "bytes32", "name": "contractID", "type": "bytes32"}],
    "name": "getCollateral",
    "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": true, "internalType": "address", "name": "publisher", "type": "address"},
      {"indexed": true, "internalType": "address", "name": "provider", "type": "address"}
    ],
    "name": "ContractCreated",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256"}
    ],
    "name": "EscrowFunded",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256"}
    ],
    "name": "CollateralDeposited",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "uint64", "name": "deadline", "type": "uint64"}
    ],
    "name": "ContractActivated",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "uint64", "name": "sequence", "type": "uint64"},
      {"indexed": false, "internalType": "uint256", "name": "cumulativePayment", "type": "uint256"}
    ],
    "name": "PaymentStateAccepted",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "uint256", "name": "providerPayment", "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "publisherRefund", "type": "uint256"}
    ],
    "name": "ContractSettled",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "enum EscrowTypes.FailureReason", "name": "reason", "type": "uint8"}
    ],
    "name": "FailureMarked",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "uint256", "name": "penalty", "type": "uint256"}
    ],
    "name": "CollateralSlashed",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"}
    ],
    "name": "ContractTerminated",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "internalType": "bytes32", "name": "contractID", "type": "bytes32"},
      {"indexed": false, "internalType": "uint64", "name": "sequence", "type": "uint64"}
    ],
    "name": "FailureDisputed",
    "type": "event"
  }
]`

// solidityPaymentState represents the struct EscrowTypes.PaymentState on-chain.
type solidityPaymentState struct {
	ContractID        [32]byte
	Publisher         common.Address
	Provider          common.Address
	Sequence          uint64
	Period            uint64
	CumulativePayment *big.Int
	LastChallengeID   [32]byte
	ValidUntil        uint64
	Status            uint8
	Signature         []byte
}

// EscrowAdapter is the production Ethereum-RPC client adapter for EscrowContract.sol.
// It implements payment.EscrowSubmitter, payment.EscrowSettler, and payment.EscrowFailureHandler.
//
// In the optimistic off-chain payment mechanism:
//   - Zero on-chain transactions occur during regular challenge rounds.
//   - Signed PaymentState vouchers are accumulated off-chain by the provider.
//   - On-chain submission and settlement are executed at the end of the contract.
//   - MarkFailure and SlashCollateral are only invoked during a dispute when
//     the consecutive failure threshold is exceeded.
type EscrowAdapter struct {
	client   *ethclient.Client
	contract *bind.BoundContract
	auth     *bind.TransactOpts
}

// DialEscrowAdapter connects to an Ethereum node and binds to a deployed EscrowContract.
func DialEscrowAdapter(
	ctx context.Context,
	rpcURL string,
	contractAddress common.Address,
	senderPrivateKey *ecdsa.PrivateKey,
	chainID *big.Int,
) (*EscrowAdapter, error) {
	if rpcURL == "" || contractAddress == (common.Address{}) || senderPrivateKey == nil || chainID == nil || chainID.Sign() <= 0 {
		return nil, errors.New("RPC URL, contract address, sender key, and positive chain ID are required")
	}

	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, err
	}

	parsedABI, err := abi.JSON(strings.NewReader(escrowABI))
	if err != nil {
		client.Close()
		return nil, err
	}

	auth, err := bind.NewKeyedTransactorWithChainID(senderPrivateKey, chainID)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &EscrowAdapter{
		client:   client,
		contract: bind.NewBoundContract(contractAddress, parsedABI, client, client, client),
		auth:     auth,
	}, nil
}

// Close closes the underlying Ethereum RPC client.
func (a *EscrowAdapter) Close() {
	if a != nil && a.client != nil {
		a.client.Close()
	}
}

// --- OPTIMISTIC PAYMENT SETTLEMENT (EscrowSubmitter & EscrowSettler) ---

// SubmitPaymentState submits a signed PaymentState to EscrowContract.sol.
// In the optimistic mechanism, this is called once at contract settlement
// (or during dispute) to record the latest accumulated voucher on-chain.
func (a *EscrowAdapter) SubmitPaymentState(contractID string, state payment.PaymentState) error {
	if a == nil || a.contract == nil || a.auth == nil || contractID != state.ContractID {
		return errors.New("matching signed payment state and escrow adapter are required")
	}

	parsedContractID := common.HexToHash(contractID)
	solidityState, err := convertPaymentStateToSolidity(parsedContractID, state)
	if err != nil {
		return err
	}

	options := *a.auth
	_, err = a.contract.Transact(&options, "submitPaymentState", parsedContractID, solidityState)
	return err
}

// SettleContract finalizes an agreement on-chain: transfers cumulativePayment
// to the provider and refunds any remaining reward to the publisher.
// Must be called with the latest accepted PaymentState after SubmitPaymentState.
func (a *EscrowAdapter) SettleContract(contractID string, state payment.PaymentState) error {
	if a == nil || a.contract == nil || a.auth == nil || contractID != state.ContractID {
		return errors.New("matching signed payment state and escrow adapter are required")
	}

	parsedContractID := common.HexToHash(contractID)
	solidityState, err := convertPaymentStateToSolidity(parsedContractID, state)
	if err != nil {
		return err
	}

	options := *a.auth
	_, err = a.contract.Transact(&options, "settleContract", parsedContractID, solidityState)
	return err
}

// --- DISPUTE & FAILURE HANDLING (EscrowFailureHandler) ---

// MarkFailure records an availability failure on-chain with default reason
// AvailabilityFailure (1). In the optimistic model, this is triggered ONLY
// when the consecutive failure threshold (K) is met.
func (a *EscrowAdapter) MarkFailure(contractID string) error {
	return a.MarkFailureWithReason(contractID, 1) // 1 = AvailabilityFailure
}

// MarkFailureWithReason records a failure on-chain with an explicit failure reason:
// 1 = AvailabilityFailure, 2 = DeadlineMissed, 3 = PaymentStateInvalid, 4 = Other.
func (a *EscrowAdapter) MarkFailureWithReason(contractID string, reason uint8) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) || reason == 0 {
		return errors.New("valid escrow adapter, contract ID, and positive failure reason are required")
	}

	options := *a.auth
	_, err := a.contract.Transact(&options, "markFailure", common.HexToHash(contractID), reason)
	return err
}

// SlashCollateral applies an eligible publisher-authorized collateral penalty (in wei).
// In the optimistic model, this is only callable after MarkFailure when a dispute
// warrants penalizing provider collateral.
func (a *EscrowAdapter) SlashCollateral(contractID string, penalty uint64) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) || penalty == 0 {
		return errors.New("valid escrow adapter, contract ID, and positive penalty are required")
	}

	options := *a.auth
	_, err := a.contract.Transact(&options, "slashCollateral", common.HexToHash(contractID), new(big.Int).SetUint64(penalty))
	return err
}

// DisputeFailure allows a provider to contest a marked failure on-chain by presenting
// their latest valid publisher-signed PaymentState voucher.
//
// In the optimistic model, the provider goes on-chain only if an unjust failure was
// marked. If the voucher signature verifies, the failure is overturned on-chain.
func (a *EscrowAdapter) DisputeFailure(contractID string, state payment.PaymentState) error {
	if a == nil || a.contract == nil || a.auth == nil || contractID != state.ContractID {
		return errors.New("matching signed payment state and escrow adapter are required")
	}

	parsedContractID := common.HexToHash(contractID)
	solidityState, err := convertPaymentStateToSolidity(parsedContractID, state)
	if err != nil {
		return err
	}

	options := *a.auth
	_, err = a.contract.Transact(&options, "disputeFailure", parsedContractID, solidityState)
	return err
}

// --- ESCROW LIFECYCLE MANAGEMENT ---

// CreateContract establishes a new agreement on EscrowContract.sol.
// It performs a static call first to return the deterministic contractID, then submits the transaction.
func (a *EscrowAdapter) CreateContract(
	provider common.Address,
	fileID [32]byte,
	reward *big.Int,
	periods uint64,
	duration uint64,
	rulesHash [32]byte,
	collateralRequired *big.Int,
) (common.Hash, error) {
	if a == nil || a.contract == nil || a.auth == nil {
		return common.Hash{}, errors.New("valid escrow adapter is required")
	}
	if provider == (common.Address{}) || fileID == [32]byte{} || reward == nil || reward.Sign() <= 0 || periods == 0 || duration == 0 {
		return common.Hash{}, errors.New("invalid contract creation parameters")
	}
	if collateralRequired == nil {
		collateralRequired = big.NewInt(0)
	}

	var callResult []interface{}
	callOpts := &bind.CallOpts{Context: a.auth.Context, From: a.auth.From}
	err := a.contract.Call(callOpts, &callResult, "createContract", provider, fileID, reward, periods, duration, rulesHash, collateralRequired)
	if err != nil {
		return common.Hash{}, fmt.Errorf("simulate createContract: %w", err)
	}
	if len(callResult) == 0 {
		return common.Hash{}, errors.New("createContract call produced no return value")
	}
	contractIDBytes, ok := callResult[0].([32]byte)
	if !ok {
		return common.Hash{}, errors.New("failed to cast createContract result to [32]byte")
	}

	options := *a.auth
	_, err = a.contract.Transact(&options, "createContract", provider, fileID, reward, periods, duration, rulesHash, collateralRequired)
	if err != nil {
		return common.Hash{}, fmt.Errorf("transact createContract: %w", err)
	}

	return common.BytesToHash(contractIDBytes[:]), nil
}

// FundEscrow deposits the required reward amount into escrow from the publisher account.
func (a *EscrowAdapter) FundEscrow(contractID string, amount *big.Int) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) || amount == nil || amount.Sign() <= 0 {
		return errors.New("valid escrow adapter, contract ID, and positive amount are required")
	}

	options := *a.auth
	options.Value = amount
	_, err := a.contract.Transact(&options, "fundEscrow", common.HexToHash(contractID))
	return err
}

// DepositCollateral deposits the required collateral amount into escrow from the provider account.
func (a *EscrowAdapter) DepositCollateral(contractID string, amount *big.Int) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) || amount == nil || amount.Sign() <= 0 {
		return errors.New("valid escrow adapter, contract ID, and positive amount are required")
	}

	options := *a.auth
	options.Value = amount
	_, err := a.contract.Transact(&options, "depositCollateral", common.HexToHash(contractID))
	return err
}

// ActivateContract activates a funded agreement and starts the availability timer.
func (a *EscrowAdapter) ActivateContract(contractID string) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return errors.New("valid escrow adapter and contract ID are required")
	}

	options := *a.auth
	_, err := a.contract.Transact(&options, "activateContract", common.HexToHash(contractID))
	return err
}

// RefundPublisher returns remaining escrowed reward to the publisher if the contract failed or terminated.
func (a *EscrowAdapter) RefundPublisher(contractID string) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return errors.New("valid escrow adapter and contract ID are required")
	}

	options := *a.auth
	_, err := a.contract.Transact(&options, "refundPublisher", common.HexToHash(contractID))
	return err
}

// ReturnCollateral returns deposited collateral to the provider after successful settlement.
func (a *EscrowAdapter) ReturnCollateral(contractID string) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return errors.New("valid escrow adapter and contract ID are required")
	}

	options := *a.auth
	_, err := a.contract.Transact(&options, "returnCollateral", common.HexToHash(contractID))
	return err
}

// TerminateExpiredContract terminates an Active contract on-chain if the active duration
// and settlement window have passed without an accepted payment state.
func (a *EscrowAdapter) TerminateExpiredContract(contractID string) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return errors.New("valid escrow adapter and contract ID are required")
	}

	options := *a.auth
	_, err := a.contract.Transact(&options, "terminateExpiredContract", common.HexToHash(contractID))
	return err
}

// --- CONTRACT QUERIES ---

// GetContractState returns the current state enum value:
// 0=Created, 1=Funded, 2=Active, 3=Failed, 4=Settled, 5=Refunded, 6=Terminated.
func (a *EscrowAdapter) GetContractState(contractID string) (uint8, error) {
	if a == nil || a.contract == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return 0, errors.New("valid escrow adapter and contract ID are required")
	}

	var callResult []interface{}
	callOpts := &bind.CallOpts{}
	if a.auth != nil {
		callOpts.Context = a.auth.Context
	}

	err := a.contract.Call(callOpts, &callResult, "getContractState", common.HexToHash(contractID))
	if err != nil {
		return 0, err
	}
	if len(callResult) == 0 {
		return 0, errors.New("empty response for getContractState")
	}

	state, ok := callResult[0].(uint8)
	if !ok {
		return 0, errors.New("failed to cast contract state to uint8")
	}
	return state, nil
}

// GetEscrowBalance returns the balance held in escrow for the agreement.
func (a *EscrowAdapter) GetEscrowBalance(contractID string) (*big.Int, error) {
	if a == nil || a.contract == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return nil, errors.New("valid escrow adapter and contract ID are required")
	}

	var callResult []interface{}
	callOpts := &bind.CallOpts{}
	if a.auth != nil {
		callOpts.Context = a.auth.Context
	}

	err := a.contract.Call(callOpts, &callResult, "getEscrowBalance", common.HexToHash(contractID))
	if err != nil {
		return nil, err
	}
	if len(callResult) == 0 {
		return nil, errors.New("empty response for getEscrowBalance")
	}

	balance, ok := callResult[0].(*big.Int)
	if !ok {
		return nil, errors.New("failed to cast escrow balance to *big.Int")
	}
	return balance, nil
}

// GetCollateral returns the amount of collateral currently held for the agreement.
func (a *EscrowAdapter) GetCollateral(contractID string) (*big.Int, error) {
	if a == nil || a.contract == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return nil, errors.New("valid escrow adapter and contract ID are required")
	}

	var callResult []interface{}
	callOpts := &bind.CallOpts{}
	if a.auth != nil {
		callOpts.Context = a.auth.Context
	}

	err := a.contract.Call(callOpts, &callResult, "getCollateral", common.HexToHash(contractID))
	if err != nil {
		return nil, err
	}
	if len(callResult) == 0 {
		return nil, errors.New("empty response for getCollateral")
	}

	collateral, ok := callResult[0].(*big.Int)
	if !ok {
		return nil, errors.New("failed to cast collateral to *big.Int")
	}
	return collateral, nil
}

// convertPaymentStateToSolidity validates and converts a high-level PaymentState
// into the exact tuple format expected by EscrowContract.sol.
func convertPaymentStateToSolidity(parsedContractID common.Hash, state payment.PaymentState) (solidityPaymentState, error) {
	publisher := common.HexToAddress(state.Publisher)
	provider := common.HexToAddress(state.Provider)

	if parsedContractID == (common.Hash{}) || publisher == (common.Address{}) || provider == (common.Address{}) || len(state.PublisherSignature) != 65 || state.ValidUntil == 0 {
		return solidityPaymentState{}, errors.New("payment state is not Ethereum-compatible")
	}

	challengeHash := crypto.Keccak256Hash([]byte(state.LastChallengeID))

	return solidityPaymentState{
		ContractID:        parsedContractID,
		Publisher:         publisher,
		Provider:          provider,
		Sequence:          state.Sequence,
		Period:            state.Period,
		CumulativePayment: new(big.Int).SetUint64(state.CumulativePayment),
		LastChallengeID:   challengeHash,
		ValidUntil:        state.ValidUntil,
		Status:            statusCode(string(state.Status)),
		Signature:         state.PublisherSignature,
	}, nil
}

// Compile-time interface checks.
var (
	_ payment.EscrowSubmitter      = (*EscrowAdapter)(nil)
	_ payment.EscrowSettler        = (*EscrowAdapter)(nil)
	_ payment.EscrowFailureHandler = (*EscrowAdapter)(nil)
)

// Unused import check guard
var _ interfaces.AvailabilityStatus

