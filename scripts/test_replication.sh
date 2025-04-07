#!/bin/bash

# Test script for replication protocol

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to check if a command succeeded
check_result() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1${NC}"
    else
        echo -e "${RED}✗ $1${NC}"
        exit 1
    fi
}

# Function to cleanup any existing server processes
cleanup() {
    echo "Cleaning up existing server processes..."
    pkill -f "bin/server" || true
    sleep 2
    # Double check if any processes are still running
    if pgrep -f "bin/server" > /dev/null; then
        echo "Force killing remaining server processes..."
        pkill -9 -f "bin/server" || true
    fi
}

# Cleanup at start
cleanup

echo "Starting replication protocol tests..."

# Build the test binary
echo "Building tests..."
go test -c ./internal/replication/...
check_result "Build tests"

# Run unit tests
echo "Running unit tests..."
go test -v ./internal/replication/...
check_result "Unit tests"

# Run integration tests
echo "Running integration tests..."
go test -v -tags=integration ./internal/replication/...
check_result "Integration tests"

# Start test cluster
echo "Starting test cluster..."
./scripts/run_cluster.sh &
check_result "Start cluster"

# Wait for cluster to start
sleep 5

# Run replication tests
echo "Running replication tests..."
go test -v -tags=integration ./internal/replication/... -run TestReplicationProtocolIntegration
check_result "Replication tests"

# Stop test cluster
echo "Stopping test cluster..."
cleanup
check_result "Stop cluster"

echo -e "${GREEN}All tests completed successfully!${NC}" 