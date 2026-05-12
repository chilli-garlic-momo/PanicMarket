#!/bin/bash
set -e

echo "=========================================="
echo "  Benchmark Platform - Dev Setup"
echo "=========================================="

# Check dependencies
check_dep() {
    if ! command -v "$1" &> /dev/null; then
        echo "ERROR: $1 not found. Please install it."
        exit 1
    fi
    echo "✓ $1 found"
}

check_dep docker
check_dep curl
check_dep jq

# Check Docker is running
if ! docker info &> /dev/null; then
    echo "ERROR: Docker daemon not running"
    exit 1
fi
echo "✓ Docker daemon running"

# Create temporal dynamic config
mkdir -p scripts/temporal-dynamicconfig
cat > scripts/temporal-dynamicconfig/development-sql.yaml << 'EOF'
limit.maxIDLength:
  - value: 255
    constraints: {}
frontend.enableServerVersionCheck:
  - value: false
    constraints: {}
EOF

# Start infrastructure
echo ""
echo "Starting infrastructure services..."
docker compose up -d postgres redis minio minio-init local-registry

echo "Waiting for PostgreSQL..."
until docker compose exec postgres pg_isready -U benchmark &> /dev/null; do
    sleep 1
done
echo "✓ PostgreSQL ready"

echo "Waiting for Redis..."
until docker compose exec redis redis-cli ping &> /dev/null; do
    sleep 1
done
echo "✓ Redis ready"

echo "Waiting for MinIO..."
until curl -sf http://localhost:9000/minio/health/live &> /dev/null; do
    sleep 1
done
echo "✓ MinIO ready"

# Start Temporal (background)
echo "Starting Temporal..."
docker compose up -d temporal
echo "Waiting for Temporal (this may take 30s)..."
sleep 15
until curl -sf http://localhost:8088 &> /dev/null 2>&1; do
    sleep 2
done
echo "✓ Temporal ready"

echo ""
echo "=========================================="
echo "  Infrastructure ready!"
echo "  Temporal UI: http://localhost:8088"
echo "  MinIO UI: http://localhost:9001"
echo "  (admin:minioadmin / minioadmin123)"
echo "=========================================="