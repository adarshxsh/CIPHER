// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

library EscrowCollateral {
    function penaltyAmount(uint256 collateral, uint256 requestedPenalty) internal pure returns (uint256) {
        return requestedPenalty > collateral ? collateral : requestedPenalty;
    }
}
