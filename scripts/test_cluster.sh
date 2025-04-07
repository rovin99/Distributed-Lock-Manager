#!/bin/bash

# Build the test client
echo "Building test client..."
go build -o bin/test_client cmd/test_client/main.go

# Test server 1
echo "Testing server 1..."
go run cmd/test_client/main.go -addr localhost:50051

# Test server 2
echo "Testing server 2..."
go run cmd/test_client/main.go -addr localhost:50052

# Test server 3
echo "Testing server 3..."
go run cmd/test_client/main.go -addr localhost:50053

echo "All tests completed." 