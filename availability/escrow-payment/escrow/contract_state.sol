// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

library EscrowTypes {
    enum ContractState { Created, Funded, Active, Failed, Settled, Refunded, Terminated }

    enum FailureReason { None, AvailabilityFailure, DeadlineMissed, PaymentStateInvalid, Other }

    struct PaymentState {
        bytes32 contractID;
        address publisher;
        address provider;
        uint64 sequence;
        uint64 period;
        uint256 cumulativePayment;
        bytes32 lastChallengeID;
        uint64 validUntil;
        uint8 status;
        bytes signature;
    }

    struct EscrowAgreement {
        address publisher;
        address provider;
        bytes32 fileID;
        bytes32 rulesHash;
        uint256 reward;
        uint256 escrowBalance;
        uint256 collateralRequired;
        uint256 collateral;
        uint64 periods;
        uint64 duration;
        uint64 activatedAt;
        uint64 deadline;
        uint64 latestSequence;
        uint256 latestCumulativePayment;
        ContractState state;
        FailureReason failureReason;
        bool exists;
        bool hasPaymentState;
    }
}
