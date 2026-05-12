#!/bin/bash
set -e

echo "Building all services..."

# Build example engine first (needed for testing)
echo "Building example trading engine..."
cd examples/rust-starter
docker build -t localhost:5001/trading-engine-example:latest .
docker push localhost:5001/trading-engine-example:latest || true
cd ../..

# Build platform services
echo "Building platform services..."
docker compose build --parallel api-gateway build-worker orchestrator bot-worker scoring-engine leaderboard-api

echo "✓ All services built"