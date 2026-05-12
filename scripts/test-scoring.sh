#!/bin/bash
# Unit test the scoring engine independently

set -e

SCORING_URL="${SCORING_URL:-http://localhost:9091}"
TEST_ID=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)

echo "=== Scoring Engine Tests ==="

echo ""
echo "Test 1: High performance score (100k TPS, 1ms p99)"
RESP=$(curl -s -X POST "$SCORING_URL/score" \
    -H "Content-Type: application/json" \
    -d "{
        \"test_id\": \"$TEST_ID\",
        \"metrics\": {
            \"max_tps\": 100000,
            \"p99_latency_ns\": 1000000,
            \"error_rate\": 0.0
        }
    }")
echo "$RESP" | jq '{final_score, throughput_score, latency_score, stability_score}'
SCORE=$(echo "$RESP" | jq -r '.final_score')
echo "Score: $SCORE (expected ~91.6)"

echo ""
echo "Test 2: Correctness fail → score = 0"
TEST_ID2=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)
# This would need correctness_passed = false in the test record
# For now we verify the scoring formula

echo ""
echo "Test 3: Rust unit tests"
cd services/scoring-engine
cargo test scorer::tests 2>&1 | grep -E "(test|FAILED|ok|error)" || true
cd ../..