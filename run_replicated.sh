#!/bin/bash

# Make sure proto code is up to date
protoc --go_out=. --go-grpc_out=. proto/lock.proto

# Build the server binary
go build -o bin/lock_server cmd/server/main.go

# Create data and logs directory if they don't exist
mkdir -p data
mkdir -p logs

# Kill any existing server processes
pkill -f lock_server || true
sleep 1

# Start the primary server
echo "Starting primary server on port 50051..."
bin/lock_server --role primary --id 1 --address :50051 --peer localhost:50052 > logs/primary.log 2>&1 &
PRIMARY_PID=$!
echo "Primary server started with PID $PRIMARY_PID"

# Wait a bit for the primary to start
sleep 2

# Start the secondary server
echo "Starting secondary server on port 50052..."
bin/lock_server --role secondary --id 2 --address :50052 --peer localhost:50051 > logs/secondary.log 2>&1 &
SECONDARY_PID=$!
echo "Secondary server started with PID $SECONDARY_PID"

echo "Both servers are now running in the background."
echo "To monitor primary: tail -f logs/primary.log"
echo "To monitor secondary: tail -f logs/secondary.log"
echo "To stop servers: pkill -f lock_server"

echo 
echo "You can test failover by killing the primary server with:"
echo "kill $PRIMARY_PID" 