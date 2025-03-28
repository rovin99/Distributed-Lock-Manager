package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constants for retry mechanism
const (
	initialTimeout = 2 * time.Second // Initial timeout per attempt
	initialBackoff = 2 * time.Second // Initial backoff delay
	maxRetries     = 5               // Maximum retry attempts
)

// LockClient wraps the gRPC client functionality
type LockClient struct {
	conn           *grpc.ClientConn
	client         pb.LockServiceClient
	id             int32
	sequenceNumber uint64     // Added for request IDs
	mu             sync.Mutex // Protects sequenceNumber
}

// NewLockClient creates a new client connected to the server
func NewLockClient(serverAddr string, clientID int32) (*LockClient, error) {
	// Establish a connection to the server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	// Create a gRPC client instance
	client := pb.NewLockServiceClient(conn)

	return &LockClient{
		conn:           conn,
		client:         client,
		id:             clientID,
		sequenceNumber: 0, // Initialize sequence number
	}, nil
}

// GenerateRequestID creates a unique request ID
func (c *LockClient) GenerateRequestID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sequenceNumber++
	return fmt.Sprintf("%d-%d", c.id, c.sequenceNumber)
}

// retryRPC is a generic function that executes an RPC with retries and exponential backoff
func (c *LockClient) retryRPC(operation string, rpcFunc func(context.Context) (*pb.Response, error)) (*pb.Response, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Create a context with timeout for this attempt
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		// Execute the RPC
		resp, err := rpcFunc(ctx)

		// Check for success
		if err == nil && resp.Status == pb.Status_SUCCESS {
			cancel() // Remember to cancel the context on success
			return resp, nil
		}

		// Save error for return if all attempts fail
		if err != nil {
			lastErr = fmt.Errorf("%s failed: %v", operation, err)
		} else {
			lastErr = fmt.Errorf("%s failed with status: %v", operation, resp.Status)
		}

		// Cancel the context for this attempt
		cancel()

		// Calculate backoff (2^attempt * initialBackoff)
		backoff := initialBackoff * time.Duration(1<<uint(attempt))
		if backoff > 30*time.Second {
			backoff = 30 * time.Second // Cap the maximum backoff
		}

		fmt.Printf("Retry %d for %s after %v delay\n", attempt+1, operation, backoff)
		time.Sleep(backoff)
	}

	return nil, fmt.Errorf("failed after %d attempts: %v", maxRetries, lastErr)
}

// retryInitClose is a specialized version for init/close operations that return StatusMsg
func (c *LockClient) retryInitClose(operation string, rpcFunc func(context.Context) (*pb.StatusMsg, error)) (*pb.StatusMsg, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		resp, err := rpcFunc(ctx)
		if err == nil {
			cancel()
			return resp, nil
		}

		lastErr = fmt.Errorf("%s failed: %v", operation, err)
		cancel()

		backoff := initialBackoff * time.Duration(1<<uint(attempt))
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}

		fmt.Printf("Retry %d for %s after %v delay\n", attempt+1, operation, backoff)
		time.Sleep(backoff)
	}

	return nil, fmt.Errorf("failed after %d attempts: %v", maxRetries, lastErr)
}

// Initialize initializes the client with the server (with retry)
func (c *LockClient) Initialize() error {
	resp, err := c.retryInitClose("ClientInit", func(ctx context.Context) (*pb.StatusMsg, error) {
		return c.client.ClientInit(ctx, &pb.Int{Rc: c.id})
	})

	if err != nil {
		return err
	}

	fmt.Printf("Server response: %s\n", resp.Message)
	return nil
}

// AcquireLock attempts to acquire the lock (with retry)
func (c *LockClient) AcquireLock() error {
	requestId := c.GenerateRequestID()
	lockArgs := &pb.LockArgs{
		ClientId:  c.id,
		RequestId: requestId,
	}

	fmt.Printf("Sending lock_acquire request (ID: %s)\n", requestId)

	_, err := c.retryRPC("LockAcquire", func(ctx context.Context) (*pb.Response, error) {
		return c.client.LockAcquire(ctx, lockArgs)
	})

	return err
}

// AppendFile appends data to a file (with retry)
func (c *LockClient) AppendFile(filename string, content []byte) error {
	requestId := c.GenerateRequestID()
	fileArgs := &pb.FileArgs{
		Filename:  filename,
		Content:   content,
		ClientId:  c.id,
		RequestId: requestId,
	}

	fmt.Printf("Sending file_append request (ID: %s)\n", requestId)

	_, err := c.retryRPC("FileAppend", func(ctx context.Context) (*pb.Response, error) {
		return c.client.FileAppend(ctx, fileArgs)
	})

	return err
}

// ReleaseLock releases the lock (with retry)
func (c *LockClient) ReleaseLock() error {
	requestId := c.GenerateRequestID()
	lockArgs := &pb.LockArgs{
		ClientId:  c.id,
		RequestId: requestId,
	}

	fmt.Printf("Sending lock_release request (ID: %s)\n", requestId)

	_, err := c.retryRPC("LockRelease", func(ctx context.Context) (*pb.Response, error) {
		return c.client.LockRelease(ctx, lockArgs)
	})

	return err
}

// Close closes the client connection (with retry)
func (c *LockClient) Close() error {
	resp, err := c.retryInitClose("ClientClose", func(ctx context.Context) (*pb.StatusMsg, error) {
		return c.client.ClientClose(ctx, &pb.Int{Rc: c.id})
	})

	if err != nil {
		return err
	}

	fmt.Printf("Server response: %s\n", resp.Message)
	return c.conn.Close()
}

// AcquireLockWithRetry is kept for backward compatibility
func (c *LockClient) AcquireLockWithRetry(maxAttempts int) error {
	return c.AcquireLock()
}
