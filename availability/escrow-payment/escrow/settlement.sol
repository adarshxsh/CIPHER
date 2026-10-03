// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./contract_state.sol";

library EscrowSettlement {
    function isNextPaymentState(
        EscrowTypes.EscrowAgreement storage agreement,
        EscrowTypes.PaymentState calldata paymentState
    ) internal view returns (bool) {
        return paymentState.sequence > agreement.latestSequence
            && paymentState.cumulativePayment >= agreement.latestCumulativePayment
            && paymentState.cumulativePayment <= agreement.reward;
    }
}
