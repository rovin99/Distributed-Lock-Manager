# Distributed Lock Manager

A distributed lock manager implementation in Go that provides mutual exclusion for clients accessing shared resources.

## Overview

This project implements a distributed lock manager with the following components:
- **Server**: Manages locks and file operations with robust error handling
- **Client**: Connects to the server to acquire locks and perform file operations
- **Lock Manager**: Handles lock acquisition and release with FIFO fairness and timeout support
- **File Manager**: Manages file operations with fine-grained per-file locking for optimal concurrency

## Features

- Distributed lock acquisition and release with FIFO queue-based fairness
- Context-based timeout support for all lock operations
- Fine-grained per-file locking for concurrent access to different files
- Comprehensive error handling and structured logging
- Efficient resource management for file handles and goroutines
- Robust handling of client disconnections
- Modular code organization for maintainability
- Extensive test suite including stress tests and benchmarks

## Project Structure

```
├── cmd
│   ├── client
│   │   └── main.go
│   └── server
│       └── main.go
├── data
├── go.mod
├── go.sum
├── internal
│   ├── client
│   │   └── client.go
│   ├── file_manager
│   │   ├── file_manager.go
│   │   └── file_manager_test.go
│   ├── lock_manager
│   │   ├── lock_manager.go
│   │   └── lock_manager_test.go
│   └── server
│       └── server.go
├── logs
├── Makefile
├── proto
│   ├── lock_grpc.pb.go
│   ├── lock.pb.go
│   └── lock.proto
└── README.md
```

## Prerequisites

- Go 1.16 or higher
- Protocol Buffers compiler (protoc)
- gRPC Go plugins

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/Distributed-Lock-Manager.git
cd Distributed-Lock-Manager
```

2. Install dependencies:
```bash
go mod download
```

3. Generate protocol buffer code (if needed):
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
go run cmd/client/main.go -port 50051 1 "This is client 1's message"
```

Parameters:
- `port`: Optional port number to connect to (default: 50051)
- `client_id`: Optional integer ID for the client (default: 1)
- `message`: Optional message to write to the file (default: "Hello, World!")

## Testing

Run the tests for each package:
```bash
# Test the lock manager
go test -v ./internal/lock_manager

# Test the file manager
go test -v ./internal/file_manager

```

## How It Works

1. The server initializes the lock manager and file manager
2. Clients connect to the server via gRPC
3. Clients must acquire a lock before performing file operations
4. Only one client can hold the lock at a time, with requests processed in FIFO order
5. After completing operations, clients release the lock
6. If a client disconnects while holding a lock, the lock is automatically released

## Architecture

The system is designed with a modular architecture:

- **Server**: Acts as the central coordinator, handling client requests and delegating to specialized components
- **Lock Manager**: Dedicated component for managing distributed locks with thread-safe operations, FIFO queuing, and timeout support
- **File Manager**: Specialized component for handling file operations with per-file locking for optimal concurrency

This modular approach improves maintainability and makes the codebase easier to extend.

## Protocol

The system uses gRPC with Protocol Buffers for communication. The main operations are:
- `client_init`: Initialize a client connection
- `lock_acquire`: Acquire the distributed lock
- `lock_release`: Release the distributed lock
- `file_append`: Append data to a file (requires lock)
- `client_close`: Close the client connection
