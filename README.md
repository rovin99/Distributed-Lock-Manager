# Distributed Lock Manager

A robust distributed lock manager implementation in Go that provides mutual exclusion for clients accessing shared resources, with comprehensive fault tolerance for network failures and node crashes.


## Overview

This project implements a distributed lock manager with the following key components:

- **Server**: Central coordinator that manages locks and file operations with robust error handling and fault tolerance
- **Client**: Connects to the server to acquire locks and perform file operations with automatic retry mechanisms
- **Lock Manager**: Handles lock acquisition and release with FIFO fairness, timeout support, and lease-based locking
- **File Manager**: Manages file operations with fine-grained per-file locking and token-based validation
- **Write-Ahead Log (WAL)**: Ensures data durability and crash recovery

## Project Structure

```
├── cmd/                      # Command-line applications
│   ├── client/               # Client application
│   │   └── main.go           # Client entry point
│   └── server/               # Server application
│       └── main.go           # Server entry point
├── data/                     # Data directory for file storage
│   └── lock_state.json       # Lock state persistence file
├── internal/                 # Private application code
│   ├── client/               # Client library implementation
│   │   └── client.go         # Client functionality
│   ├── file_manager/         # File management component
│   │   ├── filemanager.go    # File manager implementation
│   │   └── filemanager_test.go # File manager tests
│   ├── lock_manager/         # Lock management component
│   │   ├── lockmanager.go    # Lock manager implementation
│   │   ├── lockmanager_test.go # Lock manager tests
│   │   └── persistence_test.go # Persistence tests
│   ├── server/               # Server implementation
│   │   ├── request_cache.go  # Request caching for idempotency
│   │   └── server.go         # Server functionality
│   ├── retry_test.go         # Retry mechanism tests
│   └── wal/                  # Write-ahead logging component
│       ├── wal.go            # WAL implementation
│       └── wal_test.go       # WAL tests
├── logs/                     # Log files directory
├── proto/                    # Protocol buffer definitions
│   ├── lock.proto            # Service and message definitions
│   ├── lock.pb.go            # Generated Go code for messages
│   └── lock_grpc.pb.go       # Generated Go code for gRPC
├── Makefile                  # Build and run commands
├── README.md                 # Project documentation
└── run.sh                    # Script to run multiple clients
```

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
This will run multple clients concurrently, each writing to a random file.

## Testing

Run the tests for each package:
```bash
# Test the lock manager
go test -v ./internal/lock_manager

# Test the file manager
go test -v ./internal/file_manager

# Test retry mechanism and fault tolerance
go test -v ./internal
```



## How It Works

1. The server initializes the lock manager and file manager with fault tolerance features
2. Clients connect to the server via gRPC with retry mechanisms
3. Clients must acquire a lock before performing file operations
4. Only one client can hold the lock at a time, with requests processed in FIFO order
5. Locks are managed with lease-based timeouts to prevent deadlocks
6. All operations are idempotent to handle network failures
7. Token-based validation ensures only the current lock holder can modify files
8. If a client disconnects while holding a lock, the lease timeout ensures the lock is released

## Protocol

The system uses gRPC with Protocol Buffers for communication. The main operations are:

- `client_init`: Initialize a client connection
- `lock_acquire`: Acquire the distributed lock (with retry)
- `lock_release`: Release the distributed lock (with token validation)
- `file_append`: Append data to a file (requires valid lock token)
- `client_close`: Close the client connection
- `renew_lease`: Renew the lock lease to prevent timeout

Each operation includes:
- Unique request IDs for deduplication
- Token validation for security
- Retry mechanisms for reliability
- Proper error handling and status codes
- Request caching for idempotency and consistency across RPCs
