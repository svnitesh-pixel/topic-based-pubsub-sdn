#!/bin/bash

cd "$(dirname "$0")" || exit 1

mkdir -p logs/automation

LOG="logs/automation/integration_$(date +%Y%m%d_%H%M%S).log"
BROKER_LOG="logs/automation/broker.log"
SUB_LOG="logs/automation/subscriber.log"

echo "======================================"
echo " PUB-SUB INTEGRATION TEST"
echo "======================================"

# Check that required ports are available
for port in 5000 5001 5002 5003; do
    if ss -ltn | grep -q ":$port "; then
        echo "[FAIL] Port $port is already in use."
        echo "Stop the existing process before running this test."
        exit 1
    fi
done

# Start broker
echo "[1/4] Starting broker..."

python3 src/broker.py --host 127.0.0.1 --quiet \
    > "$BROKER_LOG" 2>&1 &

BROKER_PID=$!

cleanup() {
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
}

trap cleanup EXIT

# Wait for broker
for i in {1..20}; do
    if ss -ltn | grep -q ':5000 '; then
        break
    fi
    sleep 0.5
done

if ! ss -ltn | grep -q ':5000 '; then
    echo "[FAIL] Broker did not start."
    cat "$BROKER_LOG"
    exit 1
fi

echo "[PASS] Broker started"

# Start subscriber
echo "[2/4] Starting subscriber..."

python3 src/subscriber.py \
    --broker 127.0.0.1 \
    --id automation-sub \
    --topics sports \
    --count 1 \
    --duration 10 \
    > "$SUB_LOG" 2>&1 &

SUB_PID=$!

sleep 1

# Publish message
echo "[3/4] Publishing test message..."

if ! python3 src/publisher.py \
    --broker 127.0.0.1 \
    --id automation-pub \
    --topic sports \
    --message "Automated integration test successful" \
    --count 1; then
    echo "[FAIL] Publisher failed."
    exit 1
fi

# Wait for subscriber
wait "$SUB_PID"
SUB_STATUS=$?

echo "[4/4] Checking received message..."

if grep -q "RECEIVED" "$SUB_LOG"; then
    echo "[PASS] Subscriber received the message."
    echo
    cat "$SUB_LOG"
    echo
    echo "[PASS] INTEGRATION TEST SUCCESSFUL"
else
    echo "[FAIL] Subscriber did not receive the message."
    cat "$SUB_LOG"
    exit 1
fi

echo
echo "Subscriber exit status: $SUB_STATUS"
echo "Subscriber log: $SUB_LOG"
echo "Broker log: $BROKER_LOG"

echo "Integration test completed."
