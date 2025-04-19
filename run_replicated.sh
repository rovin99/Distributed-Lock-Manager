#!/bin/bash

# Stop on error
set -e

# Default port values
PORT_1=50051
PORT_2=50052
PORT_3=50053

# Process command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --port1)
      PORT_1="$2"
      shift 2
      ;;
    --port2)
      PORT_2="$2"
      shift 2
      ;;
    --port3)
      PORT_3="$2"
      shift 2
      ;;
    --skip-verifications)
      SKIP_VERIFICATIONS="--skip-verifications"
      shift
      ;;
    --help)
      echo "Usage: $0 [--port1 PORT] [--port2 PORT] [--port3 PORT] [--skip-verifications]"
      echo "  --port1 PORT           Port for server 1 (default: 50051)"
      echo "  --port2 PORT           Port for server 2 (default: 50052)"
      echo "  --port3 PORT           Port for server 3 (default: 50053)"
      echo "  --skip-verifications   Skip server ID and filesystem verifications"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Define server addresses
SERVER_1_ADDR="localhost:$PORT_1"
SERVER_2_ADDR="localhost:$PORT_2"
SERVER_3_ADDR="localhost:$PORT_3"
ALL_SERVERS="$SERVER_1_ADDR,$SERVER_2_ADDR,$SERVER_3_ADDR"

# Print the configuration
echo "Starting 3-node replicated lock server with configuration:"
echo "  Server 1 port: $PORT_1 (Primary)"
echo "  Server 2 port: $PORT_2 (Secondary)"
echo "  Server 3 port: $PORT_3 (Secondary)"
if [ -n "$SKIP_VERIFICATIONS" ]; then
  echo "  Skipping verifications: yes"
else
  echo "  Skipping verifications: no"
fi

# Create data directory if it doesn't exist
mkdir -p data
mkdir -p logs

# Clean up existing state
echo "Cleaning up existing state..."
rm -f data/lock_state.json
rm -f data/processed_requests.log
rm -f data/file_*
rm -f logs/*.log

# Build the binaries
echo "Building binaries..."
make build

# Start Server 1 (ID 1) - initially Primary
echo "Starting Server 1 (Primary) on port $PORT_1..."
./bin/server \
  --address ":$PORT_1" \
  --id 1 \
  --servers "$ALL_SERVERS" \
  $SKIP_VERIFICATIONS \
  > logs/server1.log 2>&1 &
SERVER_1_PID=$!
echo "Server 1 started with PID $SERVER_1_PID"

# Wait a second before starting the next server
sleep 1

# Start Server 2 (ID 2) - initially Secondary
echo "Starting Server 2 (Secondary) on port $PORT_2..."
./bin/server \
  --address ":$PORT_2" \
  --id 2 \
  --servers "$ALL_SERVERS" \
  $SKIP_VERIFICATIONS \
  > logs/server2.log 2>&1 &
SERVER_2_PID=$!
echo "Server 2 started with PID $SERVER_2_PID"

# Wait a second before starting the next server
sleep 1

# Start Server 3 (ID 3) - initially Secondary
echo "Starting Server 3 (Secondary) on port $PORT_3..."
./bin/server \
  --address ":$PORT_3" \
  --id 3 \
  --servers "$ALL_SERVERS" \
  $SKIP_VERIFICATIONS \
  > logs/server3.log 2>&1 &
SERVER_3_PID=$!
echo "Server 3 started with PID $SERVER_3_PID"

echo ""
echo "All three servers are running. Use the client with --servers option to connect."
echo "Example client command:"
echo "  ./bin/client --servers \"$ALL_SERVERS\" acquire"
echo ""
echo "To stop the servers, press Ctrl+C"

# Set up trap to kill all servers on exit
trap "echo 'Stopping servers...'; kill $SERVER_1_PID $SERVER_2_PID $SERVER_3_PID 2>/dev/null || true" EXIT

# Wait for Ctrl+C
wait 