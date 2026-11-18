#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR" || exit 1

REPORT_DIR="$PROJECT_DIR/logs/automation"
mkdir -p "$REPORT_DIR"

REPORT="$REPORT_DIR/report_$(date +%Y%m%d_%H%M%S).txt"


PASS=0
FAIL=0

echo "=================================="
echo " PUBLISH-SUBSCRIBE SDN AUTOMATION"
echo "=================================="
echo "Date: $(date)"
echo

check_command() {
    if command -v "$1" >/dev/null 2>&1; then
        echo "[PASS] $1 installed"
        ((PASS++))
    else
        echo "[FAIL] $1 missing"
        ((FAIL++))
    fi
}

echo "1. ENVIRONMENT CHECK"

check_command python3
check_command git
check_command make
check_command mn
check_command ovs-vsctl

if [ -x "$HOME/ryu-venv/bin/ryu-manager" ]; then
    echo "[PASS] Ryu environment found"
    ((PASS++))
else
    echo "[FAIL] Ryu environment missing"
    ((FAIL++))
fi

echo
echo "2. RUNNING UNIT TESTS"

if python3 -m unittest discover -s tests -v; then
    echo "[PASS] Unit tests"
    ((PASS++))
else
    echo "[FAIL] Unit tests"
    ((FAIL++))
fi

echo
echo "3. CHECKING PROJECT FILES"

for file in src/broker.py src/publisher.py src/subscriber.py sdn/ryu_pubsub_controller.py README.md; do
    if [ -f "$file" ]; then
        echo "[PASS] $file exists"
        ((PASS++))
    else
        echo "[FAIL] $file missing"
        ((FAIL++))
    fi
done

echo
echo "4. GIT STATUS"
git status --short

echo
echo "=================================="
echo " FINAL AUTOMATION REPORT"
echo "Passed: $PASS"
echo "Failed: $FAIL"
echo "Report: $REPORT"
echo "=================================="

echo
echo "=================================="
echo "6. RUNNING INTEGRATION TEST"
echo "=================================="

if bash smoke_test.sh; then
    echo "[PASS] Integration test"
else
    echo "[FAIL] Integration test"
    exit 1
fi

echo
echo "ALL AUTOMATED CHECKS COMPLETED"
