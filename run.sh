#!/bin/bash

# Number of concurrent clients
NUM_CLIENTS=10

# Number of files (file_0 to file_99)
NUM_FILES=100

# Server port
PORT=50051

# Message to append
MESSAGE="Concurrent multi-file test"

# Run multiple clients in parallel
for ((i=1; i<=NUM_CLIENTS; i++)); do
    
        # Pick a random file (file_0 to file_99)
        FILE_INDEX=$((RANDOM % NUM_FILES))
        FILE_NAME="file_$FILE_INDEX"
        echo "Client $i writing to $FILE_NAME..."
        go run cmd/client/main.go -port "$PORT" "$i" "$MESSAGE" "$FILE_NAME" &
done

# Wait for all background processes to finish
wait

echo "All clients finished execution."
