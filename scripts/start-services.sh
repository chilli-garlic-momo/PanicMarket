#!/bin/bash
set -e

echo "Starting all platform services..."
docker compose up -d api-gateway build-worker orchestrator bot-worker scoring-engine leaderboard-api

echo "Waiting for services to be healthy..."
sleep 5

check_service() {
    local name=$1
    local url=$2
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -sf "$url" &> /dev/null; then
            echo "✓ $name ready"
            return 0
        fi
        sleep 2
        ((attempt++))
    done

    echo "✗ $name failed to start"
    docker compose logs "$name" | tail -20
    return 1
}

check_service "api-gateway" "http://localhost:8080/health"
check_service "bot-worker" "http://localhost:9090/health"
check_service "scoring-engine" "http://localhost:9091/health"
check_service "leaderboard-api" "http://localhost:9092/health"

echo ""
echo "=========================================="
echo "  Platform is running!"
echo ""
echo "  API Gateway:     http://localhost:8080"
echo "  Leaderboard:     http://localhost:9092"
echo "  Temporal UI:     http://localhost:8088"
echo "  MinIO UI:        http://localhost:9001"
echo "=========================================="