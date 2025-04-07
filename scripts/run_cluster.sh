#!/bin/bash

# Kill any existing server processes
pkill -f "server"

# Start three servers
echo "Starting server 1..."
go run cmd/server/main.go -id server1 -addr :50051 -peers "server2:localhost:50052,server3:localhost:50053" &

sleep 5

echo "Starting server 2..."
go run cmd/server/main.go -id server2 -addr :50052 -peers "server1:localhost:50051,server3:localhost:50053" &

sleep 5

echo "Starting server 3..."
go run cmd/server/main.go -id server3 -addr :50053 -peers "server1:localhost:50051,server2:localhost:50052" &

# Wait for servers to start
sleep 6

echo "All servers started. Press Ctrl+C to stop."
wait 