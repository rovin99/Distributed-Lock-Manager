# Distributed Lock Manager

A robust distributed lock manager implementation in Go that provides mutual exclusion for clients accessing shared resources, with comprehensive fault tolerance for network failures and node crashes.


## Overview

This project implements a distributed lock manager with the following key components:

- **Server**: Central coordinator that manages locks and file operations with robust error handling and fault tolerance
- **Client**: Connects to the server to acquire locks and perform file operations with automatic retry mechanisms
- **Lock Manager**: Handles lock acquisition and release with FIFO fairness, timeout support, and lease-based locking
- **File Manager**: Manages file operations with fine-grained per-file locking and token-based validation
- **Write-Ahead Log (WAL)**: Ensures data durability and crash recovery


## Key Features

### Core Features
- **Distributed Lock Management**: 
  - FIFO queue-based fairness for lock acquisition
  - Lease-based locking with automatic timeout
  - Token-based validation for secure operations
  - Background lease monitoring and cleanup

- **File Operations**:
  - Fine-grained per-file locking for concurrent access
  - Append-only file operations with atomic writes
  - Efficient file handle management with connection pooling
  - Automatic file creation and validation

- **Fault Tolerance**:
  - Write-ahead logging for operation recovery
  - State persistence for crash recovery
  - Request caching for idempotency
  - Graceful recovery of lock state

### Advanced Features
- **Network Failure Handling**:
  - Retry mechanism with exponential backoff for packet loss
  - Idempotent operations to handle duplicate requests
  - Request deduplication using unique request IDs
  - Configurable retry parameters and timeouts

- **Client Crash Protection**:
  - Automatic lock release on lease expiration
  - Background lease monitoring and cleanup
  - Token-based lock validation
  - Graceful handling of client disconnections

- **Server Crash Recovery**:
  - Write-ahead logging for operation recovery
  - State persistence for crash recovery
  - Request caching for idempotency
  - Graceful recovery of lock state

## Architecture

The system is designed with a modular architecture focusing on fault tolerance:

### Server Component
- **Lock Manager**: 
  - Lease-based locking with timeouts
  - FIFO queuing for fairness
  - Token-based validation
  - Background lease monitoring
  - State persistence for recovery

- **File Manager**: 
  - Per-file locking for concurrency
  - Token-based access control
  - Write-ahead logging support
  - Efficient file handle management

- **Request Cache**:
  - Deduplication of requests
  - Idempotency support
  - Consistent response handling

### Client Component
- **Retry Mechanism**:
  - Exponential backoff for network failures
  - Configurable retry parameters
  - Unique request ID generation

- **Lease Management**:
  - Automatic lease renewal
  - Background renewal goroutine
  - Graceful error recovery

- **File Operations**:
  - Atomic append operations
  - Token validation
  - Error handling and recovery

## Prerequisites

- Go 1.16 or higher
- Protocol Buffers compiler (protoc)
- gRPC Go plugins

## Installation

1. Clone the repository:
```bash
git clone https://github.com/rovin99/Distributed-Lock-Manager.git
cd Distributed-Lock-Manager
```

2. Install Protocol Buffers compiler and Go plugins:
```bash
# Install protobuf compiler
sudo apt install protobuf-compiler

# Install Go protobuf plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Add Go bin directory to PATH
# For fish shell:
set -Ux fish_user_paths (go env GOPATH)/bin $fish_user_paths
# For bash/zsh:
export PATH="$PATH:$(go env GOPATH)/bin"
```

3. Install dependencies:
```bash
go mod download
go mod tidy
```

4. Generate protocol buffer code (if needed):
```bash
protoc --go_out=. --go-grpc_out=. proto/lock.proto
```

## Running the Application

1. Build the application:
```bash
make
```

2. Start the Server:
```bash
make run-server PORT=50051
```
The server will start listening on port 50051 and create 100 files (file_0 to file_99) in the data directory.

3. Run a Client:
```bash
make run-client PORT=50051
```

Or with specific parameters:
```bash
go run cmd/client/main.go -port 50051 1 "This is client 1's message" "file_9"
```

Parameters:
- `port`: Optional port number to connect to (default: 50051)
- `client_id`: Optional integer ID for the client (default: 1)
- `message`: Optional message to write to the file (default: "Hello, World!")
- `file number`: Optional file name to append the message (default: file_0)

4. Run Multiple Clients Concurrently:
```bash
make run-multi-clients
```
This will run a bash file which will spawn multple clients concurrently, each writing to a random file.

## Running Servers and Clients Individually

You can run servers and clients directly using the binary executables for more control over the setup.

### Running Servers

Prerequisites:
```bash
# Create required directories
mkdir -p data logs bin

# Build the binaries
make build
```

Start Server 1 (Primary):
```bash
./bin/server --address ":50051" --id 1 --servers "localhost:50051,localhost:50052,localhost:50053" --http-port 9081
```

Start Server 2 (Replica):
```bash
./bin/server --address ":50052" --id 2 --servers "localhost:50051,localhost:50052,localhost:50053" --http-port 9082
```

Start Server 3 (Replica):
```bash
./bin/server --address ":50053" --id 3 --servers "localhost:50051,localhost:50052,localhost:50053" --http-port 9083
```

To run in background with logs:
```bash
./bin/server --address ":50051" --id 1 --servers "localhost:50051,localhost:50052,localhost:50053" --http-port 9081 > logs/server1.log 2>&1 &
```

Server command options:
```
-address string        Address to listen on (default ":50051")
-http-port int         Port for HTTP monitoring (if 0, uses 8081 + server ID)
-id int                Server ID (1, 2, 3, ...) (default 1)
-metrics-address       Address for metrics server (set to empty to disable) (default ":8080")
-recovery-timeout      Timeout for WAL recovery (default 30s)
-servers string        Comma-separated list of all server addresses (default "localhost:50051,localhost:50052,localhost:50053")
-skip-verifications    Skip ID and filesystem verifications
```

### Running Clients

Acquire a lock:
```bash
./bin/client --servers "localhost:50051,localhost:50052,localhost:50053" --client-id 101 acquire
```

Release a lock:
```bash
./bin/client --servers "localhost:50051,localhost:50052,localhost:50053" --client-id 101 release
```

Append to a file:
```bash
./bin/client --servers "localhost:50051,localhost:50052,localhost:50053" --client-id 101 --file file_test --content "Test content" append
```

Hold a lock (will keep it until timeout or interrupt):
```bash
./bin/client --servers "localhost:50051,localhost:50052,localhost:50053" --client-id 101 hold
```

Client command options:
```
--servers string       Comma-separated list of server addresses (default "localhost:50051")
--client-id int        Client ID (default 1)
--file string          File to append to (default "file_0")
--content string       Content to append to file (default "test content")
--timeout duration     Timeout for the operation (default 1m0s)
--repeat int           Number of times to repeat the operation (default 1)
--interval duration    Interval between repeated operations (default 5s)
```

### Monitoring Server Status

You can check server status via the HTTP monitoring endpoint:
```bash
curl http://localhost:9081/status
```

Expected output for primary:
```
Server ID: 1
Role: primary
Primary: true
```

Expected output for replicas:
```
Server ID: 2
Role: secondary
Primary: false
```

## Testing

Run the tests for each package:
```bash
# Test the lock manager and persistence
go test -v ./internal/lock_manager

# Test the file manager
go test -v ./internal/file_manager

# Test client retry handling
go test -v ./internal/client

# Test write-ahead logging
go test -v ./internal/wal

# Test server functionality
go test -v ./internal/server

# Test retry mechanism and fault tolerance
go test -v ./internal

# Run all tests in the project
go test -v ./...
```

You can also run tests with race condition detection:
```bash
go test -race -v ./...
```

To run tests with coverage reporting:
```bash
go test -cover -v ./...
```

## Specialized Test Functions

The system includes several specialized test functions to verify distributed behavior and fault tolerance:

### run_enhanced_replication_test
Tests basic state replication from Primary to Replicas for lock operations.
- **Scenario**: Start cluster (S1=Primary, S2/S3=Replicas)
- **Actions**: Client acquires and releases lock via Primary
- **Verification**: Replicas correctly reflect lock state changes
- **Purpose**: Ensures Primary properly sends state updates and Replicas apply them

### run_fencing_test
Tests the fencing mechanism during Primary failover.
- **Scenario**: Start cluster, kill Primary, promote new Primary
- **Actions**: Clients attempt operations during and after fencing period
- **Verification**: Requests rejected during fencing, accepted after fencing ends
- **Purpose**: Validates promotion triggers fencing, fencing blocks writes, state is cleared post-fencing

### run_expanded_failover_test
Tests client behavior during Primary failure.
- **Scenario**: Client holds lock, Primary fails, new Primary is promoted
- **Actions**: Wait for fencing to end, verify client continuation
- **Verification**: Client handles failover transparently, new Primary serves requests correctly
- **Purpose**: Ensures client library properly handles failover and lease renewal works across failures

### run_improved_split_brain_test
Tests split-brain prevention during network partitions.
- **Scenario**: Primary isolated from majority, new Primary elected
- **Actions**: Clients interact with new Primary, network partition healed
- **Verification**: Old Primary steps down, clients find correct Primary
- **Purpose**: Ensures majority partition elects new leader, isolated Primary steps down

### run_lease_expiry_test
Tests lease expiration behavior across failover.
- **Scenario**: Client acquires lock, Primary fails, new Primary promoted
- **Actions**: Wait for lease to expire on new Primary
- **Verification**: New client can acquire lock after original lease expires
- **Purpose**: Ensures lease expiry works correctly across failures

### run_quorum_loss_test
Tests system behavior during majority node failure.
- **Scenario**: Majority of nodes fail, leaving Primary without quorum
- **Actions**: Clients attempt operations, nodes restored
- **Verification**: Primary stops serving writes without quorum, resumes when quorum restored
- **Purpose**: Validates quorum awareness and write availability guarantees

## How It Works

1. The server initializes the lock manager and file manager with fault tolerance features
2. Clients connect to the server via gRPC with retry mechanisms
3. Clients must acquire a lock before performing file operations
4. Only one client can hold the lock at a time, with requests processed in FIFO order
5. Locks are managed with lease-based timeouts to prevent deadlocks
6. All operations are idempotent to handle network failures
7. Token-based validation ensures only the current lock holder can modify files
8. If a client disconnects while holding a lock, the lease timeout ensures the lock is released

## Goal 1: Liveness via Retries (Packet Loss)

### Problem
- Client sends a request (e.g., LockAcquire) but request packet gets lost
- OR server processes request but response packet is lost
- Client waits indefinitely for a response that never arrives
- System becomes blocked (liveness failure)
- Other clients may be prevented from getting locks

### Solution
Implemented retry mechanisms in the client RPC library with:
- **Per-attempt timeouts**: 2-second deadline per RPC call
- **Multiple retries**: Up to 5 attempts per operation
- **Exponential backoff**: Increasing delays between retries (2s, 4s, 8s, 16s...)
- **Maximum retry cap**: Capped at 30s backoff and 5 total attempts
- **Definitive failure**: Returns ServerUnavailableError after all retries fail

### Example Flow: Lost Response Scenario
```
Attempt 1 (t=0s):
- Client sends LockAcquire request
- Server grants lock with token "T1"
- Response packet is lost
- Client context times out after 2s
- Client schedules retry with 2s backoff

Attempt 2 (t=4s):
- Client retries with same requestID
- Server recognizes duplicate request
- Server returns same token "T1"
- Response reaches client successfully
- Client proceeds with lock acquired
```

## Goal 2: Safety via Idempotency (Duplicate Requests)

### Problem
- Network issues cause clients to send duplicate requests
- Naive request handling can cause incorrect behavior:
  - FileAppend: Appending the same data twice
  - LockAcquire: Issues with lock state
  - LockRelease: Releasing already released locks

### Solution
Implemented idempotency mechanisms:
- **Unique Request IDs**: Client generates IDs combining client ID, UUID, and sequence number
- **Request Cache**: In-memory cache for runtime idempotency
- **Lock Manager Idempotency**: Built-in handling for duplicate lock operations
- **Processed Requests Log**: Persistent log for file operations across restarts

### Example Flow: Duplicate FileAppend
```
t=1s: Client sends FileAppend (ReqID: "Req123", Data: "data1")
t=2s: Server processes request, writes data, caches response
      Server records "Req123" in processed_requests.log
t=3s: Response is lost
t=6s: Client retries with same ReqID "Req123"
t=7s: Server finds "Req123" in cache, returns cached response
      WITHOUT appending data again
```

Alternative (Server Restart):
```
- Server crashes after processing "Req123"
- Server restarts, loads processed_requests.log
- Client retries with "Req123"
- Server checks processed requests, finds "Req123"
- Server returns success WITHOUT re-appending data
```

## Goal 3: Liveness via Leases (Client Crash)

### Problem
- Client acquires lock then crashes before releasing
- Lock remains held indefinitely
- Other clients blocked forever (liveness failure)

### Solution
Implemented lease-based lock expiration:
- **Lease Grant**: Server grants time-limited lease with lock (e.g., 30s)
- **Client Renewal**: Client background goroutine renews lease periodically (e.g., every 10s)
- **Server Monitoring**: Background goroutine checks for expired leases every second
- **Automatic Release**: Server automatically releases locks with expired leases

### Example Flow: Client Crash Scenario
```
12:00:00: Client 1 requests lock
12:00:01: Server grants lock to Client 1 (expires 12:00:31)
12:00:05: Client 1 crashes, can't send renewals
12:00:15: Client 2 requests lock, must wait
12:00:31: Server detects expired lease
12:00:31: Server automatically releases Client 1's lock
12:00:31: Client 2 acquires the lock
```

## Goal 4: Safety via Fencing (Stale Lock Prevention)

### Problem
- Client acquires lock, gets delayed (network partition/GC pause/crash)
- Its lease expires and lock is granted to another client
- Original client recovers and still believes it holds the lock
- Without protection, it could interfere with the new lock holder's work

### Solution
Implemented token-based fencing mechanism:
- **Unique Tokens**: Server generates new UUID token for each lock grant
- **Token Validation**: Server requires matching token for all lock operations
- **Time-based Validation**: Server verifies lease hasn't expired
- **Strict Rejection**: Requests with stale/invalid tokens are rejected

### Example Flow: Fencing a Stale Client
```
10:00:00: Client 1 acquires lock with token "UUID-AAA"
10:00:30: Client 1 gets network partitioned, can't renew lease
10:00:32: Server expires Client 1's lease 
10:00:34: Server grants lock to Client 2 with token "UUID-BBB"
10:00:35: Client 2 successfully appends file using "UUID-BBB"
10:00:40: Client 1 recovers, still has "UUID-AAA"
10:00:41: Client 1 attempts file append with "UUID-AAA"
10:00:42: Server rejects with INVALID_TOKEN (token doesn't match)
10:00:43: Client 1 attempts to release lock with "UUID-AAA"
10:00:44: Server rejects (client ID doesn't match current holder)
```

## Goal 5: Server Crash Recovery (Lock State Persistence)

### Problem
- Server crashes and loses in-memory lock state
- Without persistence, server would restart with empty state
- Original lock holder still believes it holds the lock
- Safety violations could occur

### Solution
Implemented persistent lock state:
- **State File**: Uses `./data/lock_state.json` to store lock holder, token, and expiry
- **Atomic Saves**: Writes to temporary file, syncs, then atomically renames
- **Startup Recovery**: Loads state on startup, validates lease expiry
- **Automatic Cleanup**: Releases expired locks detected during recovery

### Example Flow: Server Crash & Recovery
```
11:00:10: Client 1 acquires lock, server saves state to disk
          {holder: 1, token: "T-ABC", expires: 11:00:40}
11:00:20: Server crashes
11:00:50: Server restarts and loads state from disk
11:00:51: Server detects loaded lease already expired
          Releases lock and updates state file
          {holder: -1, token: "", expires: Zero}
11:00:56: Client 2 acquires lock with new token "T-DEF"
11:01:00: Client 1 attempts to renew with old token "T-ABC"
11:01:01: Server rejects with INVALID_TOKEN
```
## Protocol

The system uses gRPC with Protocol Buffers (`proto/lock.proto`). Key RPCs:

-   `ClientInit`: Basic client registration/hello.
-   `LockAcquire`: Attempts to acquire the global lock, returns status and unique lock token on success.
-   `LockRelease`: Releases the lock; requires the valid token matching the current holder.
-   `FileAppend`: Appends data; requires the valid token matching the current holder.
-   `RenewLease`: Extends the lease duration; requires the valid token matching the current holder.

Each request includes:
- Client ID.
- Unique Request ID (for idempotency).
- Lock Token (for `LockRelease`, `FileAppend`, `RenewLease`).

Responses include:
- Status code (`OK`, `ERROR`, `INVALID_TOKEN`, etc.).
- Error message on failure.
- Lock Token (for successful `LockAcquire`).
```


