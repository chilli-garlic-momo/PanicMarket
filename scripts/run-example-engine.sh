#!/bin/bash
# Run the example trading engine locally for development/testing

set -e

echo "Starting example trading engine on port 8085..."

# Build if needed
cd examples/rust-starter

# Check if binary exists
if [ ! -f "target/release/trading-engine" ]; then
    echo "Building example engine..."
    cargo build --release
fi

PORT=8085 cargo run --release &
ENGINE_PID=$!

echo "Engine PID: $ENGINE_PID"
echo "Waiting for engine to start..."

for i in {1..15}; do
    if curl -sf "http://localhost:8085/health" &>/dev/null; then
        echo "✓ Engine ready at http://localhost:8085"
        echo "  Health: http://localhost:8085/health"
        echo "  Trading: ws://localhost:8085/trading"
        echo ""
        echo "Press Ctrl+C to stop"
        wait $ENGINE_PID
        break
    fi
    sleep 1
done