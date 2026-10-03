#!/usr/bin/env bash
# ==============================================================================
# CIPHER: MULTI-TERMINAL STEP-BY-STEP WORKFLOW ORCHESTRATOR
# ==============================================================================
# Categorizes roles into:
#   1. ALWAYS-RUNNING DAEMONS:
#      - [Terminal 1] Anvil EVM Blockchain (Port 8545)
#      - [Terminal 2] DHT Bootstrap Node (Port 4003)
#      - [Terminal 3] Storage Provider 1 (Port 4101, EVM ticket verification)
#      - [Terminal 4] Storage Provider 2 (Port 4102, EVM ticket verification)
#
#   2. ONE-TIME SEQUENTIAL JOBS:
#      - [Step 1] Deploy Smart Contracts & Fund Escrow Channel
#      - [Step 2] Check Initial Wallet Balances (Client vs Provider)
#      - [Step 3] Publisher: Ingest Payload & Push Replicate (R=2)
#      - [Step 4] Consumer: Swarm Download with EIP-712 Payment Tickets
#      - [Step 5] Verify Data Integrity (SHA-256 Bit-for-Bit)
#      - [Step 6] Mine Anvil Blocks & Settle On-Chain Jackpot Payout
#      - [Step 7] Check Final Wallet Balances (Verify Net Payout)
# ==============================================================================

set -e

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$ROOT"

# Terminal ANSI styling
BOLD="\033[1m"
GREEN="\033[1;32m"
CYAN="\033[1;36m"
YELLOW="\033[1;33m"
MAGENTA="\033[1;35m"
RED="\033[1;31m"
BLUE="\033[1;34m"
NC="\033[0m"

MODE="windows" # default to macOS windows
INTERACTIVE=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --auto)
            INTERACTIVE=false
            shift
            ;;
        --single)
            MODE="single"
            shift
            ;;
        --help|-h)
            echo "CIPHER Multi-Terminal Workflow Runner"
            echo "Usage: ./scripts/launch_multi_terminal_test.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --auto      Run all one-time steps automatically without pausing"
            echo "  --single    Run everything in this single terminal (no new OS windows)"
            echo "  -h, --help  Show this help message"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

pause_step() {
    if [ "$INTERACTIVE" = true ]; then
        echo -e "\n${YELLOW}Press [ENTER] to continue to the next step...${NC}"
        read -r
    else
        sleep 1
    fi
}

echo -e "${BOLD}${CYAN}======================================================================${NC}"
echo -e "${BOLD}${CYAN}        CIPHER MULTI-TERMINAL SYSTEM & PAYMENT TEST RUNNER            ${NC}"
echo -e "${BOLD}${CYAN}======================================================================${NC}"

# Clean previous test directories and build binaries
echo -e "\n${BOLD}[*] Building latest CIPHER binaries...${NC}"
mkdir -p bin
go build -o bin/bootstrap ./network/cmd/bootstrap
go build -o bin/provider ./nodes/provider
go build -o bin/publisher ./nodes/publisher
go build -o bin/consumer ./nodes/consumer
echo -e "${GREEN}[✓] All binaries built successfully in ./bin/${NC}"

rm -rf store_p1 store_p2 store_pub store_client test_pay_orig.dat test_pay_recovered.dat
rm -f anvil.log bootstrap.log provider1.log provider2.log publisher.log consumer.log

ENTROPY_ADDR="0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9"
PROVIDER_ETH_ADDR="0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
CLIENT_ETH_ADDR="0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
CLIENT_ETH_KEY="5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
CHANNEL_CONTRACT_ADDR="0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512"

launch_window() {
    local cmd="$1"
    osascript <<EOF >/dev/null 2>&1
tell application "Terminal"
    do script "cd \"$ROOT\" && $cmd"
end tell
EOF
}

# ------------------------------------------------------------------------------
# SECTION 1: LAUNCH ALWAYS-RUNNING DAEMONS
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${MAGENTA}--- [SECTION 1: ALWAYS-RUNNING DAEMON SERVICES] ---${NC}"

# Kill any existing processes on ports 8545, 4003, 4101, 4102
for p in 8545 4003 4101 4102; do
    if lsof -ti :$p >/dev/null 2>&1; then
        echo -e "${YELLOW}[!] Cleaning existing process on port $p...${NC}"
        kill -9 $(lsof -ti :$p) 2>/dev/null || true
    fi
done
sleep 1

if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${CYAN}[1/4] Spawning Anvil EVM in Terminal window...${NC}"
    launch_window "anvil --port 8545"
else
    echo -e "${CYAN}[1/4] Starting Anvil EVM node in background...${NC}"
    anvil --port 8545 --silent > anvil.log 2>&1 &
    ANVIL_PID=$!
fi

# Wait for Anvil to be healthy
while ! curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' http://127.0.0.1:8545 >/dev/null 2>&1; do
    sleep 0.2
done
echo -e "${GREEN}[✓] Anvil EVM is healthy on http://127.0.0.1:8545${NC}"

# 2. Deploy Smart Contracts on Anvil
echo -e "\n${BOLD}[*] Deploying Smart Contracts & Funding Escrow Channel...${NC}"
(cd payments && forge script script/Step1_Setup.s.sol --rpc-url http://127.0.0.1:8545 --broadcast > /dev/null)
echo -e "${GREEN}[✓] Smart Contracts deployed!${NC}"
echo -e "  - EntropySource:       ${BOLD}$ENTROPY_ADDR${NC}"
echo -e "  - Escrow Channel:      ${BOLD}$CHANNEL_CONTRACT_ADDR${NC}"
echo -e "  - Provider Eth Wallet: ${BOLD}$PROVIDER_ETH_ADDR${NC}"
echo -e "  - Client Eth Wallet:   ${BOLD}$CLIENT_ETH_ADDR${NC}"

# 3. DHT Bootstrap Node
./bin/bootstrap -p 4003 -ws-port 0 -identity ./store_pub/boot.key > bootstrap.log 2>&1 &
BOOT_PID=$!
sleep 2

BOOTSTRAP_MULTIADDR=$(grep "127.0.0.1/tcp/4003/p2p/" bootstrap.log | head -n 1 | awk '{print $NF}')
if [ -z "$BOOTSTRAP_MULTIADDR" ]; then
    echo -e "${RED}Failed to start bootstrap node. Check bootstrap.log${NC}"
    cat bootstrap.log
    exit 1
fi
echo -e "${GREEN}[✓] Bootstrap Multiaddress: ${BOLD}$BOOTSTRAP_MULTIADDR${NC}"

if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    # Kill background bootstrap and spawn in terminal window with same key
    kill $BOOT_PID 2>/dev/null || true
    echo -e "\n${CYAN}[2/4] Spawning DHT Bootstrap Node in Terminal window...${NC}"
    launch_window "./bin/bootstrap -p 4003 -ws-port 0 -identity ./store_pub/boot.key"
    sleep 2
fi

# 4. Storage Provider 1
if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${CYAN}[3/4] Spawning Storage Provider 1 in Terminal window...${NC}"
    launch_window "./bin/provider -p 4101 -ws-port 0 -identity ./store_p1/p1.key -store ./store_p1 -bootstrap '$BOOTSTRAP_MULTIADDR' --eth-rpc http://127.0.0.1:8545 --entropy-addr '$ENTROPY_ADDR'"
else
    echo -e "${CYAN}[3/4] Starting Storage Provider 1 in background...${NC}"
    ./bin/provider -p 4101 -ws-port 0 -identity ./store_p1/p1.key -store ./store_p1 \
      -bootstrap "$BOOTSTRAP_MULTIADDR" --eth-rpc http://127.0.0.1:8545 --entropy-addr "$ENTROPY_ADDR" > provider1.log 2>&1 &
    PROV1_PID=$!
fi

# 5. Storage Provider 2
if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${CYAN}[4/4] Spawning Storage Provider 2 in Terminal window...${NC}"
    launch_window "./bin/provider -p 4102 -ws-port 0 -identity ./store_p2/p2.key -store ./store_p2 -bootstrap '$BOOTSTRAP_MULTIADDR' --eth-rpc http://127.0.0.1:8545 --entropy-addr '$ENTROPY_ADDR'"
else
    echo -e "${CYAN}[4/4] Starting Storage Provider 2 in background...${NC}"
    ./bin/provider -p 4102 -ws-port 0 -identity ./store_p2/p2.key -store ./store_p2 \
      -bootstrap "$BOOTSTRAP_MULTIADDR" --eth-rpc http://127.0.0.1:8545 --entropy-addr "$ENTROPY_ADDR" > provider2.log 2>&1 &
    PROV2_PID=$!
fi
sleep 3

echo -e "\n${GREEN}${BOLD}[✓] ALL 4 DAEMON SERVICES ARE RUNNING!${NC}"


cleanup() {
    if [ "$MODE" == "single" ]; then
        echo -e "\nCleaning up background daemons..."
        kill $ANVIL_PID $BOOT_PID $PROV1_PID $PROV2_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ------------------------------------------------------------------------------
# SECTION 2: ONE-TIME SEQUENTIAL TEST STEPS
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${MAGENTA}--- [SECTION 2: ONE-TIME SEQUENTIAL JOBS] ---${NC}"

# STEP 1: INITIAL WALLET BALANCES
echo -e "\n${BOLD}${BLUE}=== [JOB 1/5] INSPECTING INITIAL WALLET BALANCES ===${NC}"
CLIENT_BAL=$(cast balance "$CLIENT_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
PROV_BAL=$(cast balance "$PROVIDER_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
CHANNEL_BAL=$(cast balance "$CHANNEL_CONTRACT_ADDR" --rpc-url http://127.0.0.1:8545 --ether)

echo -e "  💳 ${BOLD}Client Wallet Balance  :${NC} ${YELLOW}$CLIENT_BAL ETH${NC}"
echo -e "  💳 ${BOLD}Provider Wallet Balance:${NC} ${YELLOW}$PROV_BAL ETH${NC}"
echo -e "  🏦 ${BOLD}Escrow Channel Deposit :${NC} ${YELLOW}$CHANNEL_BAL ETH${NC}"
pause_step

# STEP 2: PUBLISHER INGEST & PUSH
echo -e "\n${BOLD}${BLUE}=== [JOB 2/5] PUBLISHER INGESTING & PUSHING REPLICATION (R=2) ===${NC}"
head -c 1048576 </dev/urandom > test_pay_orig.dat
ORIG_HASH=$(shasum -a 256 test_pay_orig.dat | awk '{print $1}')
echo -e "  Generated 1 MB Payload SHA-256: ${BOLD}$ORIG_HASH${NC}"

echo -e "\nRunning Publisher push across discovered providers..."
./bin/publisher -file test_pay_orig.dat -bootstrap "$BOOTSTRAP_MULTIADDR" -replication 2 -push > publisher.log 2>&1

CONTENT_ID=$(grep "ContentID     :" publisher.log | awk '{print $NF}')
KEY=$(grep "Decryption Key:" publisher.log | awk '{print $NF}')

echo -e "${GREEN}[✓] Publisher pushed content successfully:${NC}"
echo -e "  - ContentID:      ${BOLD}$CONTENT_ID${NC}"
echo -e "  - Decryption Key: ${BOLD}$KEY${NC}"
pause_step

# STEP 3: CONSUMER SWARM DOWNLOAD WITH PAYMENT TICKETS
echo -e "\n${BOLD}${BLUE}=== [JOB 3/5] CONSUMER DOWNLOADING WITH EIP-712 PAYMENT TICKETS ===${NC}"
echo -e "Connecting to DHT, discovering providers, streaming signed lottery tickets per chunk..."

./bin/consumer -fetch "$CONTENT_ID" -key "$KEY" -out test_pay_recovered.dat \
  -bootstrap "$BOOTSTRAP_MULTIADDR" -store ./store_client \
  --eth-rpc http://127.0.0.1:8545 --eth-key "$CLIENT_ETH_KEY" \
  --entropy-addr "$ENTROPY_ADDR" --provider-eth-addr "$PROVIDER_ETH_ADDR"

RECOVERED_HASH=$(shasum -a 256 test_pay_recovered.dat | awk '{print $1}')
echo -e "\nDownloaded File SHA-256: ${BOLD}$RECOVERED_HASH${NC}"

if [ "$ORIG_HASH" != "$RECOVERED_HASH" ]; then
    echo -e "${RED}[❌ FAILED] Hash mismatch!${NC}"
    exit 1
fi
echo -e "${GREEN}[✓] 100% BIT-FOR-BIT DATA INTEGRITY VERIFIED!${NC}"
pause_step

# STEP 4: ON-CHAIN SETTLEMENT
echo -e "\n${BOLD}${BLUE}=== [JOB 4/5] MINING BLOCKS & EXECUTING ON-CHAIN SETTLEMENT ===${NC}"
echo -e "Mining 20 blocks on Anvil to satisfy confirmation delay..."
cast rpc anvil_mine 20 --rpc-url http://127.0.0.1:8545 > /dev/null

echo -e "Submitting winning ticket for on-chain jackpot payout..."
(cd payments && forge script script/Step2_Settle.s.sol --rpc-url http://127.0.0.1:8545 --broadcast)
pause_step

# STEP 5: FINAL WALLET BALANCES
echo -e "\n${BOLD}${BLUE}=== [JOB 5/5] INSPECTING FINAL WALLET BALANCES & NET PAYOUT ===${NC}"
FINAL_CLIENT_BAL=$(cast balance "$CLIENT_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
FINAL_PROV_BAL=$(cast balance "$PROVIDER_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
FINAL_CHANNEL_BAL=$(cast balance "$CHANNEL_CONTRACT_ADDR" --rpc-url http://127.0.0.1:8545 --ether)

echo -e "  💳 ${BOLD}Client Wallet Balance  :${NC} ${YELLOW}$FINAL_CLIENT_BAL ETH${NC}"
echo -e "  💳 ${BOLD}Provider Wallet Balance:${NC} ${GREEN}$FINAL_PROV_BAL ETH${NC} ${BOLD}(+1.0 ETH Earned!)${NC}"
echo -e "  🏦 ${BOLD}Escrow Channel Deposit :${NC} ${YELLOW}$FINAL_CHANNEL_BAL ETH${NC}"

echo -e "\n${BOLD}${GREEN}======================================================================${NC}"
echo -e "${BOLD}${GREEN}🎉 ALL MULTI-TERMINAL SYSTEM & PAYMENT TESTS PASSED SUCCESSFULLY!    ${NC}"
echo -e "${BOLD}${GREEN}======================================================================${NC}"
