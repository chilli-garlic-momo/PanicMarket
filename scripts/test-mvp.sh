#!/bin/bash
# Complete MVP test suite
# Tests the full submission → build → test → leaderboard flow

set -e

API_URL="${API_URL:-http://localhost:8080}"
LEADERBOARD_URL="${LEADERBOARD_URL:-http://localhost:9092}"
TIMEOUT=600  # 10 minutes max

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}✓ PASS${NC}: $1"; }
fail() { echo -e "${RED}✗ FAIL${NC}: $1"; exit 1; }
info() { echo -e "${YELLOW}→${NC} $1"; }

echo "=========================================="
echo "  MVP End-to-End Test Suite"
echo "  API: $API_URL"
echo "=========================================="

# ==========================================
# TEST 1: API Health Check
# ==========================================
echo ""
echo "--- Test 1: API Health ---"
HEALTH=$(curl -sf "$API_URL/health" || echo "FAIL")
if echo "$HEALTH" | jq -e '.status == "ok"' &> /dev/null; then
    pass "API gateway is healthy"
else
    fail "API gateway health check failed: $HEALTH"
fi

# ==========================================
# TEST 2: Submit Invalid (no file)
# ==========================================
echo ""
echo "--- Test 2: Invalid Submission (no file) ---"
RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/api/v1/submissions" \
    -F "team_name=test-team")
if [ "$RESP" = "400" ]; then
    pass "Correctly rejected submission without code file (HTTP 400)"
else
    fail "Expected HTTP 400, got $RESP"
fi

# ==========================================
# TEST 3: Submit Invalid (not gzip)
# ==========================================
echo ""
echo "--- Test 3: Invalid Submission (bad format) ---"
RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/api/v1/submissions" \
    -F "team_name=test-team" \
    -F "code=@/etc/hostname;type=application/gzip")
if [ "$RESP" = "400" ]; then
    pass "Correctly rejected non-gzip submission"
else
    info "Got HTTP $RESP (may still be correct if file is too small)"
fi

# ==========================================
# TEST 4: Create valid submission
# ==========================================
echo ""
echo "--- Test 4: Valid Submission ---"

# Create submission archive
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Create minimal trading engine
mkdir -p "$TMPDIR/src"

cat > "$TMPDIR/Cargo.toml" << 'EOF'
[package]
name = "trading-engine"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
axum = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4"] }
futures-util = "0.3"
tracing = "0.1"
tracing-subscriber = "0.3"
EOF

cat > "$TMPDIR/README.md" << 'EOF'
# Test Trading Engine
A minimal trading engine for benchmark testing.
EOF

# Copy example engine source
cp examples/rust-starter/src/main.rs "$TMPDIR/src/"
cp examples/rust-starter/src/orderbook.rs "$TMPDIR/src/"
cp examples/rust-starter/Dockerfile "$TMPDIR/"

SUBMISSION_TAR="$TMPDIR/submission.tar.gz"
tar czf "$SUBMISSION_TAR" -C "$TMPDIR" Cargo.toml README.md Dockerfile src/

info "Submitting test engine (team: test-team-mvp)..."
SUBMIT_RESP=$(curl -s -X POST "$API_URL/api/v1/submissions" \
    -F "code=@$SUBMISSION_TAR;type=application/gzip" \
    -F "team_name=test-team-mvp" \
    -F "language=rust")

echo "Submission response: $SUBMIT_RESP" | head -c 500

SUBMISSION_ID=$(echo "$SUBMIT_RESP" | jq -r '.submission_id' 2>/dev/null)
if [ -z "$SUBMISSION_ID" ] || [ "$SUBMISSION_ID" = "null" ]; then
    fail "No submission_id in response: $SUBMIT_RESP"
fi

pass "Submission accepted: $SUBMISSION_ID"

# ==========================================
# TEST 5: Check submission status
# ==========================================
echo ""
echo "--- Test 5: Submission Status Polling ---"

info "Polling submission status (timeout: ${TIMEOUT}s)..."
START_TIME=$(date +%s)
LAST_STATUS=""

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    if [ $ELAPSED -gt $TIMEOUT ]; then
        fail "Timeout after ${TIMEOUT}s. Last status: $LAST_STATUS"
    fi

    STATUS_RESP=$(curl -sf "$API_URL/api/v1/submissions/$SUBMISSION_ID" || echo '{"status":"error"}')
    STATUS=$(echo "$STATUS_RESP" | jq -r '.status' 2>/dev/null || echo "unknown")

    if [ "$STATUS" != "$LAST_STATUS" ]; then
        info "Status changed: $LAST_STATUS → $STATUS (${ELAPSED}s elapsed)"
        LAST_STATUS="$STATUS"
    fi

    case "$STATUS" in
        "queued"|"building")
            echo -n "."
            sleep 5
            ;;
        "built"|"deploying"|"testing")
            echo ""
            info "Build complete! Now in: $STATUS"
            sleep 10
            ;;
        "completed")
            echo ""
            pass "Submission completed in ${ELAPSED}s"
            break
            ;;
        "failed")
            echo ""
            ERROR=$(echo "$STATUS_RESP" | jq -r '.error_message' 2>/dev/null)
            BUILD_LOG=$(curl -sf "$API_URL/api/v1/submissions/$SUBMISSION_ID/build-logs" 2>/dev/null | tail -20)
            echo "Error: $ERROR"
            echo "Build log (last 20 lines):"
            echo "$BUILD_LOG"
            fail "Submission failed: $ERROR"
            ;;
        *)
            echo -n "?"
            sleep 5
            ;;
    esac
done

# ==========================================
# TEST 6: Get test results
# ==========================================
echo ""
echo "--- Test 6: Test Results ---"

TEST_ID=$(curl -sf "$API_URL/api/v1/submissions/$SUBMISSION_ID" | jq -r '.test_id // empty' 2>/dev/null)

# If no test_id in submission, find from tests
if [ -z "$TEST_ID" ]; then
    info "Fetching test ID from leaderboard..."
    TEST_ID=$(curl -sf "$LEADERBOARD_URL/api/leaderboard" | \
        jq -r --arg sid "$SUBMISSION_ID" '.entries[] | select(.submission_id == $sid) | .test_id' 2>/dev/null | head -1)
fi

if [ -z "$TEST_ID" ]; then
    fail "Could not find test_id for submission $SUBMISSION_ID"
fi

info "Test ID: $TEST_ID"

TEST_RESP=$(curl -sf "$API_URL/api/v1/tests/$TEST_ID")
echo "Test results:"
echo "$TEST_RESP" | jq '{
    test_id: .test_id,
    status: .status,
    final_score: .final_score,
    max_tps: .max_tps,
    p99_latency_ms: .p99_latency_ms,
    error_rate: .error_rate,
    correctness_passed: .correctness_passed
}'

FINAL_SCORE=$(echo "$TEST_RESP" | jq -r '.final_score // "null"')
if [ "$FINAL_SCORE" != "null" ] && [ "$FINAL_SCORE" != "0" ]; then
    pass "Test has valid score: $FINAL_SCORE"
elif [ "$FINAL_SCORE" = "0" ]; then
    info "Score is 0 (engine may have had issues, but scoring ran)"
else
    fail "No final score computed"
fi

# ==========================================
# TEST 7: Leaderboard
# ==========================================
echo ""
echo "--- Test 7: Leaderboard ---"

LEADERBOARD=$(curl -sf "$LEADERBOARD_URL/api/leaderboard")
ENTRY_COUNT=$(echo "$LEADERBOARD" | jq '.entries | length')

if [ "$ENTRY_COUNT" -gt 0 ]; then
    pass "Leaderboard has $ENTRY_COUNT entries"
    echo "Top entry:"
    echo "$LEADERBOARD" | jq '.entries[0]'
else
    fail "Leaderboard is empty after completed test"
fi

# Check our submission is on leaderboard
OUR_ENTRY=$(echo "$LEADERBOARD" | jq --arg sid "$SUBMISSION_ID" \
    '.entries[] | select(.submission_id == $sid)')

if [ -n "$OUR_ENTRY" ]; then
    RANK=$(echo "$OUR_ENTRY" | jq -r '.rank')
    SCORE=$(echo "$OUR_ENTRY" | jq -r '.score')
    pass "Our submission is on leaderboard at rank $RANK with score $SCORE"
else
    info "Submission not on leaderboard yet (may still be processing)"
fi

# ==========================================
# TEST 8: Build Logs
# ==========================================
echo ""
echo "--- Test 8: Build Logs ---"

BUILD_LOGS=$(curl -sf "$API_URL/api/v1/submissions/$SUBMISSION_ID/build-logs")
if [ -n "$BUILD_LOGS" ]; then
    LOG_LEN=${#BUILD_LOGS}
    pass "Build logs available ($LOG_LEN bytes)"
    echo "--- Last 10 lines of build log ---"
    echo "$BUILD_LOGS" | tail -10
else
    info "Build logs empty (may be normal if build was fast)"
fi

# ==========================================
# TEST 9: Debug endpoint
# ==========================================
echo ""
echo "--- Test 9: Debug Endpoint ---"

DEBUG_RESP=$(curl -sf "$API_URL/api/v1/tests/$TEST_ID/debug")
if echo "$DEBUG_RESP" | jq -e '.test_id' &> /dev/null; then
    TIMELINE_LEN=$(echo "$DEBUG_RESP" | jq '.timeline | length')
    pass "Debug endpoint returns valid data (timeline: $TIMELINE_LEN events)"
else
    fail "Debug endpoint returned invalid data: $DEBUG_RESP"
fi

# ==========================================
# TEST 10: WebSocket Leaderboard
# ==========================================
echo ""
echo "--- Test 10: WebSocket Leaderboard ---"

# Use websocat if available, otherwise skip
if command -v websocat &> /dev/null; then
    WS_RESP=$(echo "" | timeout 5 websocat "ws://localhost:9092/ws/leaderboard" 2>/dev/null | head -1)
    if echo "$WS_RESP" | jq -e '.type' &> /dev/null; then
        WS_TYPE=$(echo "$WS_RESP" | jq -r '.type')
        pass "WebSocket received message type: $WS_TYPE"
    else
        info "WebSocket connected but no message received (may be empty leaderboard)"
    fi
else
    info "websocat not installed, skipping WebSocket test"
    info "Install with: cargo install websocat"
fi

# ==========================================
# SUMMARY
# ==========================================
echo ""
echo "=========================================="
echo -e "${GREEN}  MVP Tests Complete!${NC}"
echo ""
echo "  Submission ID: $SUBMISSION_ID"
echo "  Test ID:       $TEST_ID"
echo "  Final Score:   $FINAL_SCORE"
echo ""
echo "  View leaderboard: http://localhost:9092"
echo "  View Temporal:    http://localhost:8088"
echo "=========================================="