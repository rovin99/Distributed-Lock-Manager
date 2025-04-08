#!/bin/bash

# Stop on error
set -e

# Default port values
PRIMARY_PORT=50051
SECONDARY_PORT=50052

# Process command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --primary-port)
      PRIMARY_PORT="$2"
      shift 2
      ;;
    --secondary-port)
      SECONDARY_PORT="$2"
      shift 2
      ;;
    --skip-verifications)
      SKIP_VERIFICATIONS="--skip-verifications"
      shift
      ;;
    --help)
      echo "Usage: $0 [--primary-port PORT] [--secondary-port PORT] [--skip-verifications]"
      echo "  --primary-port PORT     Port for primary server (default: 50051)"
      echo "  --secondary-port PORT   Port for secondary server (default: 50052)"
      echo "  --skip-verifications    Skip server ID and filesystem verifications"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Print the configuration
echo "Starting replicated lock server with configuration:"
echo "  Primary server port: $PRIMARY_PORT"
echo "  Secondary server port: $SECONDARY_PORT"
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

# Start the primary server
echo "Starting primary server on port $PRIMARY_PORT..."
./bin/server \
  --address ":$PRIMARY_PORT" \
  --role primary \
  --id 1 \
  --peer "localhost:$SECONDARY_PORT" \
  $SKIP_VERIFICATIONS \
  > logs/primary.log 2>&1 &
PRIMARY_PID=$!
echo "Primary server started with PID $PRIMARY_PID"

# Wait a second before starting the secondary
sleep 1

# Start the secondary server
echo "Starting secondary server on port $SECONDARY_PORT..."
./bin/server \
  --address ":$SECONDARY_PORT" \
  --role secondary \
  --id 2 \
  --peer "localhost:$PRIMARY_PORT" \
  $SKIP_VERIFICATIONS \
  > logs/secondary.log 2>&1 &
SECONDARY_PID=$!
echo "Secondary server started with PID $SECONDARY_PID"

echo ""
echo "Both servers are running. Use the client with --servers option to connect."
echo "Example client command:"
echo "  ./bin/client --servers \"localhost:$PRIMARY_PORT,localhost:$SECONDARY_PORT\" acquire"
echo ""
echo "To stop the servers, press Ctrl+C"

# Set up trap to kill both servers on exit
trap "echo 'Stopping servers...'; kill $PRIMARY_PID $SECONDARY_PID 2>/dev/null || true" EXIT

# Wait for Ctrl+C
wait 