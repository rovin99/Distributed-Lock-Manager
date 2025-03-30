package client

import (
	"context"
	"fmt"
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

// renewLease sends a lease renewal request to the server
func (c *LockClient) renewLease(token string) error {
	requestId := c.GenerateRequestID()
	leaseArgs := &pb.LeaseArgs{
		ClientId:  c.id,
		RequestId: requestId,
		Token:     token,
	}

	fmt.Printf("Sending renew_lease request (ID: %s)\n", requestId)

	_, err := c.retryRPC("RenewLease", func(ctx context.Context) (*pb.Response, error) {
		return c.client.RenewLease(ctx, leaseArgs)
	})

	return err
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

			// Special handling for lease expiration
			if resp.Status == pb.Status_LEASE_EXPIRED {
				c.mu.Lock()
				c.hasLock = false
				c.lockToken = ""
				c.mu.Unlock()
				c.stopLeaseRenewal()
				return nil, fmt.Errorf("lease expired, lock lost")
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

// retryAcquire is a specialized version for lock_acquire that returns a token
func (c *LockClient) retryAcquire(operation string, rpcFunc func(context.Context) (*pb.LockResponse, error)) (*pb.LockResponse, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		resp, err := rpcFunc(ctx)
		if err == nil && resp.Response.Status == pb.Status_SUCCESS {
			cancel()
			return resp, nil
		}

		if err != nil {
			lastErr = fmt.Errorf("%s failed: %v", operation, err)
		} else {
			lastErr = fmt.Errorf("%s failed with status: %v", operation, resp.Response.Status)
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
	// Stop any existing lease renewal
	c.stopLeaseRenewal()

	requestId := c.GenerateRequestID()
	lockArgs := &pb.LockArgs{
		ClientId:  c.id,
		RequestId: requestId,
	}

	fmt.Printf("Sending lock_acquire request (ID: %s)\n", requestId)

	resp, err := c.retryAcquire("LockAcquire", func(ctx context.Context) (*pb.LockResponse, error) {
		return c.client.LockAcquire(ctx, lockArgs)
	})

	if err != nil {
		return err
	}

	// Store the token
	c.mu.Lock()
	c.lockToken = resp.Token
	c.hasLock = true
	c.mu.Unlock()

	// Start lease renewal
	c.startLeaseRenewal()

	return nil
}

// AppendFile appends data to a file (with retry)
func (c *LockClient) AppendFile(filename string, content []byte) error {
	c.mu.Lock()
	if !c.hasLock || c.lockToken == "" {
		c.mu.Unlock()
		return fmt.Errorf("client does not hold a valid lock")
	}
	token := c.lockToken
	c.mu.Unlock()

	requestId := c.GenerateRequestID()
	fileArgs := &pb.FileArgs{
		Filename:  filename,
		Content:   content,
		ClientId:  c.id,
		RequestId: requestId,
		Token:     token,
	}

	fmt.Printf("Sending file_append request (ID: %s)\n", requestId)

	resp, err := c.retryRPC("FileAppend", func(ctx context.Context) (*pb.Response, error) {
		return c.client.FileAppend(ctx, fileArgs)
	})

	if err != nil {
		// If we get a PERMISSION_DENIED, our lock might have expired
		if resp != nil && resp.Status == pb.Status_PERMISSION_DENIED {
			c.mu.Lock()
			c.hasLock = false
			c.lockToken = ""
			c.mu.Unlock()
			c.stopLeaseRenewal()
		}
		return err
	}

	return nil
}

// ReleaseLock releases the lock (with retry)
func (c *LockClient) ReleaseLock() error {
	c.mu.Lock()
	if !c.hasLock || c.lockToken == "" {
		c.mu.Unlock()
		return fmt.Errorf("client does not hold a valid lock")
	}
	token := c.lockToken
	c.mu.Unlock()

	// Stop lease renewal
	c.stopLeaseRenewal()

	requestId := c.GenerateRequestID()
	lockArgs := &pb.LockArgs{
		ClientId:  c.id,
		RequestId: requestId,
		Token:     token,
	}

	fmt.Printf("Sending lock_release request (ID: %s)\n", requestId)

	_, err := c.retryRPC("LockRelease", func(ctx context.Context) (*pb.Response, error) {
		return c.client.LockRelease(ctx, lockArgs)
	})

	// Even if there was an error, clear the lock state
	c.mu.Lock()
	c.hasLock = false
	c.lockToken = ""
	c.mu.Unlock()

	return err
}

// Close closes the client connection (with retry)
func (c *LockClient) Close() error {
	// Stop lease renewal
	c.stopLeaseRenewal()

	// If we have a lock, try to release it
	c.mu.Lock()
	if c.hasLock && c.lockToken != "" {
		c.mu.Unlock()
		_ = c.ReleaseLock() // Ignore errors
	} else {
		c.mu.Unlock()
	}

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
