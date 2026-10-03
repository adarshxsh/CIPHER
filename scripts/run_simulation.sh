#!/usr/bin/env bash
# ==============================================================================
# CIPHER: DYNAMIC AVAILABILITY, DEMAND SURGE & ON-CHAIN SLASHING SIMULATOR
# ==============================================================================
# Demonstrates the full cross-domain availability lifecycle in real time:
#   1. Ingestion & Binary SHA-256 Merkle Rooting
#   2. Dynamic Consumer Demand Spikes & Hashcash PoW Verification (proof-of-request)
#   3. Dynamic Replica Scaling (R=2 -> R=4) & Signed Cache Announcements
#   4. Stochastic Multi-Provider P2P Challenges (/cipher/availability/1.0.0)
#   5. Progressive Off-Chain Micropayment Vouchers (PaymentState)
#   6. Byzantine Adversary Detection:
#        - Epoch 1: Tampered Merkle Leaf Hash (Streak: 1/3)
#        - Epoch 2: Stale Nonce Signature Replay (Streak: 2/3)
#        - Epoch 3: Chunk Dropping / Timeout (Streak: 3/3 -> SlashEligible: true)
#   7. On-Chain Smart Contract Action (EscrowContract.sol.slashCollateral)
#   8. Final Proportional Settlement (SettlementCompleted vs SettlementWithheld)
# ==============================================================================

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [ -f "$SCRIPT_DIR/go.mod" ]; then
    ROOT="$SCRIPT_DIR"
else
    ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
fi
cd "$ROOT"

EPOCHS=4
SPEED=1200
INTERACTIVE=false
MODE="dashboard"

while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--epochs)
            EPOCHS="$2"
            shift 2
            ;;
        -s|--speed)
            SPEED="$2"
            shift 2
            ;;
        -i|--interactive)
            INTERACTIVE=true
            shift
            ;;
        --multi-window)
            MODE="multi-window"
            shift
            ;;
        -h|--help)
            echo "CIPHER Availability & Slashing Simulation Runner"
            echo "Usage: ./scripts/run_simulation.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -e, --epochs <N>      Number of availability epochs to simulate (default: 4)"
            echo "  -s, --speed <MS>      Delay in milliseconds between steps (default: 1200)"
            echo "  -i, --interactive     Pause for user confirmation between epochs"
            echo "      --multi-window    Open multi-terminal visualizer on macOS (Terminal.app)"
            echo "  -h, --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1 (use --help for usage)"
            exit 1
            ;;
    esac
done

if [ "$MODE" == "multi-window" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "\033[1;36m[*] Launching multi-terminal simulator windows...\033[0m"
    osascript -e "tell application \"Terminal\" to do script \"cd '$ROOT' && go run ./scripts/simulate_availability.go -epochs $EPOCHS -speed $SPEED\""
    exit 0
fi

# Run interactive dashboard in current terminal
ARGS="-epochs $EPOCHS -speed $SPEED"
if [ "$INTERACTIVE" = true ]; then
    ARGS="$ARGS -interactive"
fi

exec go run ./scripts/simulate_availability.go $ARGS
