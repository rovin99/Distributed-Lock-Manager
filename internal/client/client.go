package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constants for retry mechanism
const (
	initialTimeout  = 2 * time.Second  // Initial timeout per attempt
	initialBackoff  = 2 * time.Second  // Initial backoff delay
	maxRetries      = 5                // Maximum retry attempts
	leaseRenewalInt = 10 * time.Second // Lease renewal interval
)

// LockClient wraps the gRPC client functionality
type LockClient struct {
	conn           *grpc.ClientConn
	client         pb.LockServiceClient
	id             int32
	sequenceNumber uint64     // Added for request IDs
	clientUUID     string     // Unique instance ID to survive crashes
	mu             sync.Mutex // Protects sequenceNumber and lockToken

	// Added for lease management
	lockToken     string        // Current lock token
	hasLock       bool          // Whether the client currently holds a lock
	leaseTimer    *time.Timer   // Timer for lease renewal
	renewalActive bool          // Whether renewal is active
	cancelRenewal chan struct{} // Channel to stop renewal goroutine
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
		sequenceNumber: 0,                   // Initialize sequence number
		clientUUID:     uuid.New().String(), // Generate a unique UUID for this client instance
		lockToken:      "",
		hasLock:        false,
		renewalActive:  false,
		cancelRenewal:  make(chan struct{}),
	}, nil
}

// GenerateRequestID creates a unique request ID
// The format combines the client ID, UUID, and sequence number to ensure
// uniqueness even if the client crashes and restarts
func (c *LockClient) GenerateRequestID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sequenceNumber++
	return fmt.Sprintf("%d-%s-%d", c.id, c.clientUUID[:8], c.sequenceNumber)
}

// startLeaseRenewal starts a background goroutine to renew the lease
func (c *LockClient) startLeaseRenewal() {
	c.mu.Lock()

	// If renewal is already active, do nothing
	if c.renewalActive {
		c.mu.Unlock()
		return
	}

	// Reset the cancel channel if it was closed before
	select {
	case <-c.cancelRenewal:
		c.cancelRenewal = make(chan struct{})
	default:
	}

	c.renewalActive = true
	currentToken := c.lockToken
	c.mu.Unlock()

	go func(token string) {
		ticker := time.NewTicker(leaseRenewalInt)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Check if we still have the same token
				c.mu.Lock()
				if !c.hasLock || c.lockToken != token {
					c.renewalActive = false
					c.mu.Unlock()
					return
				}
				currentToken := c.lockToken
				c.mu.Unlock()

				// Attempt to renew the lease
				if err := c.renewLease(currentToken); err != nil {
					fmt.Printf("Lease renewal failed: %v\n", err)
					// If renewal fails, we should stop renewal and invalidate the lock
					c.mu.Lock()
					if c.lockToken == currentToken {
						c.hasLock = false
						c.lockToken = ""
					}
					c.renewalActive = false
					c.mu.Unlock()
					return
				}

			case <-c.cancelRenewal:
				// Stop renewal was requested
				c.mu.Lock()
				c.renewalActive = false
				c.mu.Unlock()
				return
			}
		}
	}(currentToken)
}

// stopLeaseRenewal stops the background lease renewal
func (c *LockClient) stopLeaseRenewal() {
	c.mu.Lock()
	if c.renewalActive {
		close(c.cancelRenewal)
		c.renewalActive = false
	}
	c.mu.Unlock()
}

// retryRPC is a generic function that executes an RPC with retries and exponential backoff
func (c *LockClient) retryRPC(operation string, rpcFunc func(context.Context) (*pb.LockResponse, error)) (*pb.LockResponse, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Create a context with timeout for this attempt
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		// Execute the RPC
		resp, err := rpcFunc(ctx)

		// Check for success
		if err == nil && resp.Status == pb.Status_OK {
			cancel() // Remember to cancel the context on success
			return resp, nil
		}

		// Handle DeadlineExceeded error specially
		if err != nil && strings.Contains(err.Error(), "DeadlineExceeded") {
			// For lock release, if we get a DeadlineExceeded, we should verify the lock was released
			if operation == "LockRelease" {
				// Try one more time with a fresh context to verify the lock was released
				verifyCtx, verifyCancel := context.WithTimeout(context.Background(), initialTimeout)
				verifyResp, verifyErr := rpcFunc(verifyCtx)
				verifyCancel()

				// If the verification succeeds or we get a specific error indicating the lock is not held,
				// then we can assume the lock was released
				if verifyErr == nil && verifyResp.Status == pb.Status_OK ||
					(verifyErr == nil && verifyResp.Status == pb.Status_INVALID_TOKEN) {
					// Update client state since the lock was released
					c.mu.Lock()
					c.hasLock = false
					c.lockToken = ""
					c.mu.Unlock()
					c.stopLeaseRenewal()
					cancel()
					return &pb.LockResponse{Status: pb.Status_OK}, nil
				}
			}
		}

		// Save error for return if all attempts fail
		if err != nil {
			lastErr = fmt.Errorf("%s failed: %v", operation, err)
		} else {
			lastErr = fmt.Errorf("%s failed with status: %v", operation, resp.Status)

			// Special handling for token-related errors
			if resp.Status == pb.Status_INVALID_TOKEN {
				c.mu.Lock()
				c.hasLock = false
				c.lockToken = ""
				c.mu.Unlock()
				c.stopLeaseRenewal()
				return nil, fmt.Errorf("invalid token, lock lost")
			}
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

// retryLeaseRenewal is a specialized version for lease renewal
func (c *LockClient) retryLeaseRenewal(operation string, rpcFunc func(context.Context) (*pb.LeaseResponse, error)) (*pb.LeaseResponse, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		resp, err := rpcFunc(ctx)
		if err == nil && resp.Status == pb.Status_OK {
			cancel()
			return resp, nil
		}

		if err != nil {
			lastErr = fmt.Errorf("%s failed: %v", operation, err)
		} else {
			lastErr = fmt.Errorf("%s failed with status: %v", operation, resp.Status)
		}

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

// retryFileAppend is a specialized version for file append
func (c *LockClient) retryFileAppend(operation string, rpcFunc func(context.Context) (*pb.FileResponse, error)) (*pb.FileResponse, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		resp, err := rpcFunc(ctx)
		if err == nil && resp.Status == pb.Status_OK {
			cancel()
			return resp, nil
		}

		if err != nil {
			lastErr = fmt.Errorf("%s failed: %v", operation, err)
		} else {
			lastErr = fmt.Errorf("%s failed with status: %v", operation, resp.Status)
		}

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

// renewLease sends a lease renewal request to the server
func (c *LockClient) renewLease(token string) error {
	args := &pb.LeaseArgs{
		ClientId:  c.id,
		Token:     token,
		RequestId: c.GenerateRequestID(),
	}

	_, err := c.retryLeaseRenewal("RenewLease", func(ctx context.Context) (*pb.LeaseResponse, error) {
		return c.client.RenewLease(ctx, args)
	})

	if err != nil {
		return fmt.Errorf("lease renewal failed: %v", err)
	}

	return nil
}

// FileAppend attempts to append content to a file
func (c *LockClient) FileAppend(filename string, content []byte) error {
	args := &pb.FileArgs{
		ClientId:  c.id,
		Filename:  filename,
		Content:   content,
		Token:     c.lockToken,
		RequestId: c.GenerateRequestID(),
	}

	_, err := c.retryFileAppend("FileAppend", func(ctx context.Context) (*pb.FileResponse, error) {
		return c.client.FileAppend(ctx, args)
	})

	if err != nil {
		return fmt.Errorf("file append failed: %v", err)
	}

	return nil
}

// Close closes the client connection
func (c *LockClient) Close() error {
	// Stop any ongoing lease renewal
	c.stopLeaseRenewal()

	// Close the gRPC connection
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %v", err)
	}

	return nil
}

// ClientInit initializes the client with the server
func (c *LockClient) ClientInit() error {
	args := &pb.ClientInitArgs{
		ClientId:  c.id,
		RequestId: c.GenerateRequestID(),
	}

	_, err := c.retryRPC("ClientInit", func(ctx context.Context) (*pb.LockResponse, error) {
		initResp, err := c.client.ClientInit(ctx, args)
		if err != nil {
			return nil, err
		}
		return &pb.LockResponse{
			Status: initResp.Status,
		}, nil
	})

	if err != nil {
		return fmt.Errorf("client initialization failed: %v", err)
	}

	return nil
}

// LockAcquire attempts to acquire a lock
func (c *LockClient) LockAcquire() error {
	args := &pb.LockArgs{
		ClientId:  c.id,
		RequestId: c.GenerateRequestID(),
	}

	resp, err := c.retryRPC("LockAcquire", func(ctx context.Context) (*pb.LockResponse, error) {
		return c.client.LockAcquire(ctx, args)
	})

	if err != nil {
		return fmt.Errorf("lock acquisition failed: %v", err)
	}

	// Update client state with the new token
	c.mu.Lock()
	c.hasLock = true
	c.lockToken = resp.Token
	c.mu.Unlock()

	// Start lease renewal
	c.startLeaseRenewal()

	return nil
}

// LockRelease attempts to release the lock
func (c *LockClient) LockRelease() error {
	args := &pb.LockArgs{
		ClientId:  c.id,
		Token:     c.lockToken,
		RequestId: c.GenerateRequestID(),
	}

	_, err := c.retryRPC("LockRelease", func(ctx context.Context) (*pb.LockResponse, error) {
		return c.client.LockRelease(ctx, args)
	})

	if err != nil {
		return fmt.Errorf("lock release failed: %v", err)
	}

	// Update client state
	c.mu.Lock()
	c.hasLock = false
	c.lockToken = ""
	c.mu.Unlock()

	// Stop lease renewal
	c.stopLeaseRenewal()

	return nil
}

// AcquireLockWithRetry is kept for backward compatibility
func (c *LockClient) AcquireLockWithRetry() error {
	return c.LockAcquire()
}
