#!/bin/bash
# Test bot worker against a locally running trading engine
# Use this to verify bot behavior without full pipeline

set -e

BOT_URL="${BOT_URL:-http://localhost:9090}"
ENGINE_URL="${ENGINE_URL:-http://localhost:8085}"

echo "=========================================="
echo "  Bot Worker Test"
echo "  Bot: $BOT_URL"
echo "  Engine: $ENGINE_URL"
echo "=========================================="

TEST_ID=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)

echo "Starting test $TEST_ID..."

START_RESP=$(curl -s -X POST "$BOT_URL/start" \
    -H "Content-Type: application/json" \
    -d "{
        \"test_id\": \"$TEST_ID\",
        \"engine_endpoint\": \"$ENGINE_URL\",
        \"duration_secs\": 30,
        \"bot_count\": 10,
        \"target_tps\": 100
    }")

echo "Start response: $START_RESP"

echo "Running for 35s..."
sleep 35

echo "Collecting results..."
STOP_RESP=$(curl -s -X POST "$BOT_URL/stop" \
    -H "Content-Type: application/json" \
    -d "{\"test_id\": \"$TEST_ID\"}")

echo "Results:"
echo "$STOP_RESP" | jq .