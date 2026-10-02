// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./contract_state.sol";
import "./settlement.sol";
import "./collateral.sol";
import "./queries.sol";

// EscrowContract holds funds on-chain. Availability verification remains
// off-chain; an authorized publisher records the resulting contractual action.
contract EscrowContract is IEscrowQueries {
    using EscrowSettlement for EscrowTypes.EscrowAgreement;

    error UnknownContract();
    error Unauthorized();
    error InvalidState();
    error InvalidInput();
    error InvalidPaymentState();
    error InvalidSignature();
    error TransferFailed();

    mapping(bytes32 => EscrowTypes.EscrowAgreement) private agreements;
    mapping(bytes32 => EscrowTypes.PaymentState) private latestPaymentStates;
    uint256 private nextContractNonce;

    event ContractCreated(bytes32 indexed contractID, address indexed publisher, address indexed provider);
    event EscrowFunded(bytes32 indexed contractID, uint256 amount);
    event CollateralDeposited(bytes32 indexed contractID, uint256 amount);
    event ContractActivated(bytes32 indexed contractID, uint64 deadline);
    event PaymentStateAccepted(bytes32 indexed contractID, uint64 sequence, uint256 cumulativePayment);
    event ContractSettled(bytes32 indexed contractID, uint256 providerPayment, uint256 publisherRefund);
    event FailureMarked(bytes32 indexed contractID, EscrowTypes.FailureReason reason);
    event CollateralSlashed(bytes32 indexed contractID, uint256 penalty);
    event ContractTerminated(bytes32 indexed contractID);
    event FailureDisputed(bytes32 indexed contractID, uint64 sequence);

    // Settlement window duration after the active deadline during which the latest
    // off-chain voucher can be submitted on-chain and settled.
    uint64 public constant SETTLEMENT_WINDOW = 1 days;

    function createContract(
        address provider,
        bytes32 fileID,
        uint256 reward,
        uint64 periods,
        uint64 duration,
        bytes32 rulesHash,
        uint256 collateralRequired
    ) external returns (bytes32 contractID) {
        if (provider == address(0) || fileID == bytes32(0) || reward == 0 || periods == 0 || duration == 0) {
            revert InvalidInput();
        }
        contractID = keccak256(abi.encode(address(this), block.chainid, msg.sender, provider, fileID, nextContractNonce++));
        agreements[contractID] = EscrowTypes.EscrowAgreement({
            publisher: msg.sender,
            provider: provider,
            fileID: fileID,
            rulesHash: rulesHash,
            reward: reward,
            escrowBalance: 0,
            collateralRequired: collateralRequired,
            collateral: 0,
            periods: periods,
            duration: duration,
            activatedAt: 0,
            deadline: 0,
            latestSequence: 0,
            latestCumulativePayment: 0,
            state: EscrowTypes.ContractState.Created,
            failureReason: EscrowTypes.FailureReason.None,
            exists: true,
            hasPaymentState: false
        });
        emit ContractCreated(contractID, msg.sender, provider);
    }

    function fundEscrow(bytes32 contractID) external payable {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (msg.sender != agreement.publisher || agreement.state != EscrowTypes.ContractState.Created || msg.value != agreement.reward) {
            revert InvalidState();
        }
        agreement.escrowBalance = msg.value;
        agreement.state = EscrowTypes.ContractState.Funded;
        emit EscrowFunded(contractID, msg.value);
    }

    function depositCollateral(bytes32 contractID) external payable {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (msg.sender != agreement.provider || agreement.state != EscrowTypes.ContractState.Funded) revert InvalidState();
        if (agreement.collateral != 0) revert InvalidState(); // Prevent double-deposit
        if (agreement.collateralRequired == 0 || msg.value != agreement.collateralRequired) revert InvalidInput();
        agreement.collateral = msg.value;
        emit CollateralDeposited(contractID, msg.value);
    }

    function activateContract(bytes32 contractID) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (msg.sender != agreement.publisher && msg.sender != agreement.provider) revert Unauthorized();
        if (agreement.state != EscrowTypes.ContractState.Funded || agreement.escrowBalance != agreement.reward) revert InvalidState();
        if (agreement.collateral < agreement.collateralRequired) revert InvalidState();
        agreement.activatedAt = uint64(block.timestamp);
        agreement.deadline = uint64(block.timestamp) + agreement.duration;
        agreement.state = EscrowTypes.ContractState.Active;
        emit ContractActivated(contractID, agreement.deadline);
    }

    // submitPaymentState records a signed off-chain voucher on-chain.
    //
    // In the optimistic model, vouchers accumulate off-chain during the Active
    // period. After the deadline, there is a SETTLEMENT_WINDOW grace period
    // during which the provider (or publisher) can submit the latest voucher
    // on-chain before settling. Without this window, an honest provider who
    // stored files for the full term would be unable to redeem their payment.
    function submitPaymentState(bytes32 contractID, EscrowTypes.PaymentState calldata paymentState) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (agreement.state != EscrowTypes.ContractState.Active || block.timestamp > agreement.deadline + SETTLEMENT_WINDOW) revert InvalidState();
        if (paymentState.contractID != contractID || paymentState.publisher != agreement.publisher || paymentState.provider != agreement.provider || paymentState.validUntil < block.timestamp || !agreement.isNextPaymentState(paymentState)) {
            revert InvalidPaymentState();
        }
        if (_recoverPaymentSigner(paymentState) != agreement.publisher) revert InvalidSignature();
        agreement.latestSequence = paymentState.sequence;
        agreement.latestCumulativePayment = paymentState.cumulativePayment;
        agreement.hasPaymentState = true;
        latestPaymentStates[contractID] = paymentState;
        emit PaymentStateAccepted(contractID, paymentState.sequence, paymentState.cumulativePayment);
    }

    function settleContract(bytes32 contractID, EscrowTypes.PaymentState calldata paymentState) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (agreement.state != EscrowTypes.ContractState.Active || !agreement.hasPaymentState) revert InvalidState();
        if (paymentState.sequence != agreement.latestSequence || paymentState.cumulativePayment != agreement.latestCumulativePayment || paymentState.contractID != contractID) revert InvalidPaymentState();
        uint256 providerPayment = agreement.latestCumulativePayment;
        uint256 publisherRefund = agreement.escrowBalance - providerPayment;
        agreement.escrowBalance = 0;
        agreement.state = EscrowTypes.ContractState.Settled;
        _sendValue(agreement.provider, providerPayment);
        _sendValue(agreement.publisher, publisherRefund);
        emit ContractSettled(contractID, providerPayment, publisherRefund);
    }

    function refundPublisher(bytes32 contractID) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        // If contract is Active and past deadline + SETTLEMENT_WINDOW with no payment state, auto-terminate
        if (agreement.state == EscrowTypes.ContractState.Active && block.timestamp > agreement.deadline + SETTLEMENT_WINDOW && !agreement.hasPaymentState) {
            agreement.state = EscrowTypes.ContractState.Terminated;
            emit ContractTerminated(contractID);
        }
        if (msg.sender != agreement.publisher || (agreement.state != EscrowTypes.ContractState.Failed && agreement.state != EscrowTypes.ContractState.Terminated)) revert InvalidState();
        uint256 refund = agreement.escrowBalance;
        agreement.escrowBalance = 0;
        agreement.state = EscrowTypes.ContractState.Refunded;
        _sendValue(agreement.publisher, refund);
    }

    // returnCollateral returns deposited collateral to the provider.
    //
    // Allowed in terminal states once dispute/slashing resolution is complete:
    //   - Settled: normal end of contract, provider gets full collateral back.
    //   - Terminated: contract expired without payment, provider gets collateral back.
    //   - Refunded: publisher has concluded slashing/refund, any remaining un-slashed
    //               collateral is now safe to return to the provider.
    //
    // NOTE: Collateral cannot be withdrawn while in 'Failed' state to prevent
    // front-running a pending slashCollateral() call by the publisher.
    function returnCollateral(bytes32 contractID) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (
            (agreement.state != EscrowTypes.ContractState.Settled &&
             agreement.state != EscrowTypes.ContractState.Terminated &&
             agreement.state != EscrowTypes.ContractState.Refunded)
            || agreement.collateral == 0
        ) revert InvalidState();
        uint256 collateral = agreement.collateral;
        agreement.collateral = 0;
        _sendValue(agreement.provider, collateral);
    }

    function markFailure(bytes32 contractID, EscrowTypes.FailureReason reason) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (msg.sender != agreement.publisher || agreement.state != EscrowTypes.ContractState.Active || reason == EscrowTypes.FailureReason.None) revert InvalidState();
        agreement.failureReason = reason;
        agreement.state = EscrowTypes.ContractState.Failed;
        emit FailureMarked(contractID, reason);
    }

    // disputeFailure allows a provider to counter a markFailure on-chain by presenting
    // a valid publisher-signed PaymentState voucher.
    //
    // In the optimistic pipeline, challenges run off-chain to avoid gas costs.
    // The provider interacts with the blockchain only if a dispute is raised.
    // If the publisher calls markFailure() erroneously or dishonestly, the provider
    // submits their latest signed voucher.
    //
    // NOTE: The voucher must satisfy isNextPaymentState:
    //   - sequence > agreement.latestSequence
    //   - cumulativePayment >= agreement.latestCumulativePayment
    //   - cumulativePayment <= agreement.reward
    // This prevents a provider from submitting a stale, lower, or out-of-bounds voucher.
    function disputeFailure(bytes32 contractID, EscrowTypes.PaymentState calldata paymentState) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (agreement.state != EscrowTypes.ContractState.Failed) revert InvalidState();
        if (msg.sender != agreement.provider) revert Unauthorized();
        if (
            paymentState.contractID != contractID ||
            paymentState.publisher != agreement.publisher ||
            paymentState.provider != agreement.provider ||
            paymentState.validUntil < block.timestamp ||
            !agreement.isNextPaymentState(paymentState)
        ) {
            revert InvalidPaymentState();
        }
        if (_recoverPaymentSigner(paymentState) != agreement.publisher) revert InvalidSignature();

        // Valid voucher proves provider compliance: overturn the failure
        agreement.failureReason = EscrowTypes.FailureReason.None;
        agreement.latestSequence = paymentState.sequence;
        agreement.latestCumulativePayment = paymentState.cumulativePayment;
        agreement.hasPaymentState = true;
        latestPaymentStates[contractID] = paymentState;
        agreement.state = EscrowTypes.ContractState.Active;
        emit FailureDisputed(contractID, paymentState.sequence);
    }

    function slashCollateral(bytes32 contractID, uint256 penalty) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (msg.sender != agreement.publisher || agreement.state != EscrowTypes.ContractState.Failed || penalty == 0) revert InvalidState();
        uint256 amount = EscrowCollateral.penaltyAmount(agreement.collateral, penalty);
        agreement.collateral -= amount;
        _sendValue(agreement.publisher, amount);
        emit CollateralSlashed(contractID, amount);
    }

    // terminateExpiredContract transitions an Active contract to Terminated
    // after the settlement window has elapsed if no payment state was ever submitted.
    //
    // CRITICAL INVARIANT:
    // If a voucher was accepted during the settlement window (hasPaymentState == true),
    // the contract CANNOT be terminated. It must be settled via settleContract()
    // so the provider receives their earned cumulative payment.
    function terminateExpiredContract(bytes32 contractID) external {
        EscrowTypes.EscrowAgreement storage agreement = _agreement(contractID);
        if (agreement.state != EscrowTypes.ContractState.Active) revert InvalidState();
        if (block.timestamp <= agreement.deadline + SETTLEMENT_WINDOW) revert InvalidState();
        if (agreement.hasPaymentState) revert InvalidState(); // Voucher exists; must settle, cannot terminate!
        agreement.state = EscrowTypes.ContractState.Terminated;
        emit ContractTerminated(contractID);
    }

    function getContractState(bytes32 contractID) external view override returns (EscrowTypes.ContractState) { return _agreement(contractID).state; }
    function getPaymentState(bytes32 contractID) external view override returns (EscrowTypes.PaymentState memory) { _agreement(contractID); return latestPaymentStates[contractID]; }
    function getEscrowBalance(bytes32 contractID) external view override returns (uint256) { return _agreement(contractID).escrowBalance; }
    function getCollateral(bytes32 contractID) external view override returns (uint256) { return _agreement(contractID).collateral; }

    function _agreement(bytes32 contractID) private view returns (EscrowTypes.EscrowAgreement storage agreement) {
        agreement = agreements[contractID];
        if (!agreement.exists) revert UnknownContract();
    }

    function _recoverPaymentSigner(EscrowTypes.PaymentState calldata paymentState) private view returns (address) {
        if (paymentState.signature.length != 65) return address(0);
        bytes32 digest = keccak256(abi.encode(address(this), block.chainid, paymentState.contractID, paymentState.publisher, paymentState.provider, paymentState.sequence, paymentState.period, paymentState.cumulativePayment, paymentState.lastChallengeID, paymentState.validUntil, paymentState.status));
        bytes32 messageHash = keccak256(abi.encodePacked("\x19Ethereum Signed Message:\n32", digest));
        bytes calldata signature = paymentState.signature;
        bytes32 r; bytes32 s; uint8 v;
        assembly { r := calldataload(signature.offset) s := calldataload(add(signature.offset, 32)) v := byte(0, calldataload(add(signature.offset, 64))) }
        if (v < 27) v += 27;
        if (v != 27 && v != 28 || uint256(s) > 0x7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0) return address(0);
        return ecrecover(messageHash, v, r, s);
    }

    function _sendValue(address recipient, uint256 amount) private {
        if (amount == 0) return;
        (bool sent,) = recipient.call{value: amount}("");
        if (!sent) revert TransferFailed();
    }
}

