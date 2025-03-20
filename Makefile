# Makefile for Distributed Lock Manager

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get

# Binary names
SERVER_BIN=bin/server
CLIENT_BIN=bin/client

# Source files
SERVER_SRC=cmd/server/main.go
CLIENT_SRC=cmd/client/main.go

# Directories
BIN_DIR=bin
DATA_DIR=data
LOG_DIR=logs

# Default target
all: clean setup build

# Setup directories
setup:
	@mkdir -p $(BIN_DIR)
	@mkdir -p $(DATA_DIR)
	@mkdir -p $(LOG_DIR)

# Build the server and client
build: build-server build-client

build-server:
	$(GOBUILD) -o $(SERVER_BIN) $(SERVER_SRC)
	@echo "Server built successfully"

build-client:
	$(GOBUILD) -o $(CLIENT_BIN) $(CLIENT_SRC)
	@echo "Client built successfully"

# Run the server
run-server: build-server setup
	@$(SERVER_BIN) -address :$(if $(PORT),$(PORT),50051) 2>&1 | tee $(LOG_DIR)/server.log

# Run the client with optional PORT
run-client: build-client setup
	@$(CLIENT_BIN) $(if $(PORT),--port $(PORT)) 2>&1 | tee $(LOG_DIR)/client.log

# Run multiple clients concurrently
run-multi-clients: build-client setup
	@echo "Running 5 clients concurrently..."
	@mkdir -p $(LOG_DIR)/multi_clients
	@for i in 1 2 3 4 5; do \
	    ($(CLIENT_BIN) -port $(PORT) $$i "Client $$i writing" > $(LOG_DIR)/multi_clients/client_$$i.log 2>&1) & \
	done
	@echo "Waiting for all clients to complete..."
	@wait
	@echo "All clients completed"
	@echo "Output logs available in $(LOG_DIR)/multi_clients/"
	@cat $(LOG_DIR)/multi_clients/client_*.log > $(LOG_DIR)/multi_clients/combined.log
	@echo "Combined log available at $(LOG_DIR)/multi_clients/combined.log"

# Test correctness with multiple clients writing to the same file
test-correctness: build-client setup
	@echo "Testing correctness with multiple clients..."
	@touch $(DATA_DIR)/file_0
	@echo "Starting correctness test (appending to existing file)..."
	@mkdir -p $(LOG_DIR)/test_correctness
	@for i in 1 2 3 4 5; do \
	    $(CLIENT_BIN) -port $(PORT) $$i "Client $$i writing" > $(LOG_DIR)/test_correctness/client_$$i.log 2>&1 & \
	done
	@echo "Waiting for all clients to complete..."
	@wait
	@echo "All clients completed"
	@echo "Contents of file_0:"
	@cat $(DATA_DIR)/file_0
	@echo "Client logs available in $(LOG_DIR)/test_correctness/"
	@cat $(LOG_DIR)/test_correctness/client_*.log > $(LOG_DIR)/test_correctness/combined.log
	@echo "Combined log available at $(LOG_DIR)/test_correctness/combined.log"

# Clean up
clean-bin:
	@rm -rf $(BIN_DIR)
	@echo "Cleaned up binaries"

# Clean data files
clean-data:
	@rm -rf $(DATA_DIR)/*
	@echo "Cleaned up data files"

# Clean log files
clean-logs:
	@rm -rf $(LOG_DIR)/*
	@echo "Cleaned up log files"

# Clean everything
clean: clean-bin clean-data clean-logs

# Install dependencies
deps:
	$(GOGET) google.golang.org/grpc
	$(GOGET) google.golang.org/protobuf/cmd/protoc-gen-go
	$(GOGET) google.golang.org/grpc/cmd/protoc-gen-go-grpc

# Generate protobuf code
proto:
	protoc --go_out=. --go-grpc_out=. proto/lock.proto

# Help
help:
	@echo "Available commands:"
	@echo "  make all                  - Clean, setup directories, and build binaries"
	@echo "  make build                - Build server and client binaries"
	@echo "  make run-server           - Run the server from binary"
	@echo "  make run-client           - Run the client from binary"
	@echo "  make run-multi-clients    - Run multiple clients concurrently and wait for completion"
	@echo "  make test-correctness     - Test lock correctness with multiple clients (append to existing file)"
	@echo "  make test-correctness-clean - Test lock correctness with multiple clients (clean start)"
	@echo "  make clean-bin            - Remove binaries"
	@echo "  make clean-data           - Remove data files"
	@echo "  make clean-logs           - Remove log files"
	@echo "  make clean                - Remove binaries, data files, and logs"
	@echo "  make deps                 - Install dependencies"
	@echo "  make proto                - Generate protobuf code"

.PHONY: all setup build build-server build-client run-server run-client run-multi-clients test-correctness test-correctness-clean clean-bin clean-data clean-logs clean deps proto help