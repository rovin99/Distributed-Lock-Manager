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
	initialTimeout = 2 * time.Second // Initial timeout per attempt
	initialBackoff = 2 * time.Second // Initial backoff delay
	maxRetries     = 5               // Maximum retry attempts
)

// Variable for lease renewal
var leaseRenewalInt = 10 * time.Second // Lease renewal interval

// LockClient wraps the gRPC client functionality
type LockClient struct {
	conn           *grpc.ClientConn
	client         pb.LockServiceClient
	id             int32
	sequenceNumber uint64     // Added for request IDs
	clientUUID     string     // Unique instance ID to survive crashes
	mu             sync.Mutex // Protects sequenceNumber and lockToken

	// Added for high availability
	serverAddrs      []string   // List of all available server addresses
	currentServerIdx int        // Index of the current server in use
	serverMu         sync.Mutex // Protects server-related fields

	// Added for lease management
	lockToken     string        // Current lock token
	hasLock       bool          // Whether the client currently holds a lock
	leaseTimer    *time.Timer   // Timer for lease renewal
	renewalActive bool          // Whether renewal is active
	cancelRenewal chan struct{} // Channel to stop renewal goroutine
}

// NewLockClient creates a new client connected to the primary server
func NewLockClient(serverAddr string, clientID int32) (*LockClient, error) {
	return NewLockClientWithFailover([]string{serverAddr}, clientID)
}

// NewLockClientWithFailover creates a new client with multiple server addresses for failover
func NewLockClientWithFailover(serverAddrs []string, clientID int32) (*LockClient, error) {
	fmt.Printf("DEBUG: Creating client with server addresses: %v\n", serverAddrs)

	if len(serverAddrs) == 0 {
		return nil, fmt.Errorf("at least one server address must be provided")
	}

	client := &LockClient{
		id:               clientID,
		sequenceNumber:   0,
		clientUUID:       uuid.New().String(),
		serverAddrs:      serverAddrs,
		currentServerIdx: 0,
		lockToken:        "",
		hasLock:          false,
		renewalActive:    false,
		cancelRenewal:    make(chan struct{}),
	}

	fmt.Printf("DEBUG: Initial client struct created with serverAddrs=%v, currentServerIdx=%d\n",
		client.serverAddrs, client.currentServerIdx)

	// Try each server in the provided order until one succeeds
	var lastErr error
	connected := false

	for i := 0; i < len(serverAddrs); i++ {
		client.currentServerIdx = i
		fmt.Printf("DEBUG: Attempting to connect to server at index %d (%s)\n",
			client.currentServerIdx, serverAddrs[i])

		if err := client.connectToCurrentServer(); err != nil {
			lastErr = err
			fmt.Printf("DEBUG: Failed to connect to server %s: %v, trying next...\n", serverAddrs[i], err)
			continue
		}
		connected = true
		fmt.Printf("DEBUG: Successfully connected to server at index %d (%s)\n",
			client.currentServerIdx, serverAddrs[i])
		break
	}

	if !connected {
		fmt.Printf("DEBUG: Failed to connect to any server after trying all addresses\n")
		return nil, fmt.Errorf("failed to connect to any server: %w", lastErr)
	}

	return client, nil
}

// connectToCurrentServer establishes a connection to the current server
func (c *LockClient) connectToCurrentServer() error {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()

	// Close existing connection if there is one
	if c.conn != nil {
		fmt.Printf("DEBUG: Closing existing connection before connecting to new server\n")
		c.conn.Close()
		c.conn = nil
		c.client = nil
	}

	// Get current server address
	if c.currentServerIdx >= len(c.serverAddrs) {
		return fmt.Errorf("no available servers to connect to")
	}

	serverAddr := c.serverAddrs[c.currentServerIdx]
	fmt.Printf("DEBUG: Attempting to connect to server %s (index %d of %d servers)\n",
		serverAddr, c.currentServerIdx, len(c.serverAddrs))

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("DEBUG: grpc.Dial(%s) failed with error: %v\n", serverAddr, err)
		return fmt.Errorf("failed to connect to server %s: %w", serverAddr, err)
	}

	c.conn = conn
	c.client = pb.NewLockServiceClient(conn)
	fmt.Printf("DEBUG: Successfully connected to server at %s (index %d)\n", serverAddr, c.currentServerIdx)
	return nil
}

// tryNextServer attempts to connect to the next available server
func (c *LockClient) tryNextServer() error {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()

	initialIdx := c.currentServerIdx

	// Try each server in order starting from the next one
	for i := 0; i < len(c.serverAddrs); i++ {
		// Move to the next server in a circular fashion
		c.currentServerIdx = (c.currentServerIdx + 1) % len(c.serverAddrs)

		// Skip if we've come back to the server that just failed
		if c.currentServerIdx == initialIdx {
			continue
		}

		// Try to connect to this server
		if err := c.connectToCurrentServer(); err != nil {
			fmt.Printf("Failed to connect to server %s: %v\n",
				c.serverAddrs[c.currentServerIdx], err)
			continue
		}

		// Successfully connected to a server
		return nil
	}

	// If we got here, we tried all servers and none worked
	return fmt.Errorf("tried all available servers, none are reachable")
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

					// If it's a server unavailable error, try to reconnect to another server
					if IsServerUnavailable(err) {
						fmt.Println("Attempting to connect to another server...")
						if reconnErr := c.tryNextServer(); reconnErr != nil {
							fmt.Printf("Failed to connect to another server: %v\n", reconnErr)
						} else {
							// Try lease renewal again with the new server
							if err := c.renewLease(currentToken); err != nil {
								fmt.Printf("Lease renewal with new server failed: %v\n", err)
								c.invalidateLock()
							} else {
								fmt.Println("Successfully renewed lease with new server")
								continue
							}
						}
					} else {
						// For other errors, invalidate the lock
						c.invalidateLock()
					}
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

// invalidateLock clears the client's lock state
func (c *LockClient) invalidateLock() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.hasLock = false
	c.lockToken = ""
	c.renewalActive = false
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
	backoff := initialBackoff

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

		// Handle server fencing error
		if err == nil && resp.Status == pb.Status_SERVER_FENCING {
			fmt.Printf("Server is in fencing period, operation rejected\n")
			cancel()
			return nil, &ServerFencingError{
				Operation: operation,
				Message:   resp.ErrorMessage,
			}
		}

		// Handle secondary mode error (server is not primary)
		if err == nil && resp.Status == pb.Status_SECONDARY_MODE {
			fmt.Printf("Server is in secondary mode, trying next server...\n")
			cancel()

			// Try next server
			if reconnErr := c.tryNextServer(); reconnErr != nil {
				lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
				break // No more servers to try
			}
			continue // Try with new server
		}

		// Handle connection errors (server might be down)
		if err != nil && (strings.Contains(err.Error(), "connection") ||
			strings.Contains(err.Error(), "Unavailable") ||
			strings.Contains(err.Error(), "DeadlineExceeded")) {

			fmt.Printf("Connection error on attempt %d: %v, trying next server...\n", attempt+1, err)
			cancel()

			// Try next server
			if reconnErr := c.tryNextServer(); reconnErr != nil {
				lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
				break // No more servers to try
			}
			continue // Try with new server
		}

		// Handle token validation errors
		if err == nil && (resp.Status == pb.Status_INVALID_TOKEN || resp.Status == pb.Status_PERMISSION_DENIED) {
			cancel()
			c.invalidateLock() // Clear lock state
			return resp, &InvalidTokenError{Message: resp.ErrorMessage}
		}

		// For other errors, back off and retry
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("server returned status: %v, message: %s", resp.Status, resp.ErrorMessage)
		}

		fmt.Printf("Attempt %d failed: %v, retrying in %v...\n", attempt+1, lastErr, backoff)
		cancel()
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	// All retries failed
	return nil, &ServerUnavailableError{
		Operation: operation,
		Attempts:  maxRetries,
		LastError: lastErr,
	}
}

// retryLeaseRenewal is similar to retryRPC but for lease renewal operations
func (c *LockClient) retryLeaseRenewal(operation string, rpcFunc func(context.Context) (*pb.LeaseResponse, error)) (*pb.LeaseResponse, error) {
	var lastErr error
	backoff := initialBackoff

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

		// Handle server fencing error (similar to retryRPC)
		if err == nil && resp.Status == pb.Status_SERVER_FENCING {
			fmt.Printf("Server is in fencing period, operation rejected\n")
			cancel()
			return nil, &ServerFencingError{
				Operation: operation,
				Message:   resp.ErrorMessage,
			}
		}

		// Handle secondary mode error (server is not primary)
		if err == nil && resp.Status == pb.Status_SECONDARY_MODE {
			fmt.Printf("Server is in secondary mode, trying next server...\n")
			cancel()

			if reconnErr := c.tryNextServer(); reconnErr != nil {
				lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
				break // No more servers to try
			}
			continue // Try with new server
		}

		// Handle connection errors (server might be down)
		if err != nil && (strings.Contains(err.Error(), "connection") ||
			strings.Contains(err.Error(), "Unavailable") ||
			strings.Contains(err.Error(), "DeadlineExceeded")) {

			fmt.Printf("Connection error on attempt %d: %v, trying next server...\n", attempt+1, err)
			cancel()

			if reconnErr := c.tryNextServer(); reconnErr != nil {
				lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
				break
			}
			continue
		}

		// Handle token validation errors
		if err == nil && (resp.Status == pb.Status_INVALID_TOKEN || resp.Status == pb.Status_PERMISSION_DENIED) {
			cancel()
			c.invalidateLock() // Clear lock state
			return resp, &InvalidTokenError{Message: resp.ErrorMessage}
		}

		// For other errors, back off and retry
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("server returned status: %v, message: %s", resp.Status, resp.ErrorMessage)
		}

		fmt.Printf("Attempt %d failed: %v, retrying in %v...\n", attempt+1, lastErr, backoff)
		cancel()
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	// All retries failed
	return nil, &ServerUnavailableError{
		Operation: operation,
		Attempts:  maxRetries,
		LastError: lastErr,
	}
}

// retryFileAppend is a specialized retry function for file append operations
func (c *LockClient) retryFileAppend(operation string, rpcFunc func(context.Context) (*pb.FileResponse, error)) (*pb.FileResponse, error) {
	var lastErr error
	backoff := initialBackoff

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

		// Handle server fencing error
		if err == nil && resp.Status == pb.Status_SERVER_FENCING {
			fmt.Printf("Server is in fencing period, operation rejected\n")
			cancel()
			return nil, &ServerFencingError{
				Operation: operation,
				Message:   resp.ErrorMessage,
			}
		}

		// Handle secondary mode error
		if err == nil && resp.Status == pb.Status_SECONDARY_MODE {
			fmt.Printf("Server is in secondary mode, trying next server...\n")
			cancel()

			if reconnErr := c.tryNextServer(); reconnErr != nil {
				lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
				break
			}
			continue
		}

		// Handle connection errors
		if err != nil && (strings.Contains(err.Error(), "connection") ||
			strings.Contains(err.Error(), "Unavailable") ||
			strings.Contains(err.Error(), "DeadlineExceeded")) {

			fmt.Printf("Connection error on attempt %d: %v, trying next server...\n", attempt+1, err)
			cancel()

			if reconnErr := c.tryNextServer(); reconnErr != nil {
				lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
				break
			}
			continue
		}

		// Handle token validation errors
		if err == nil && (resp.Status == pb.Status_INVALID_TOKEN || resp.Status == pb.Status_PERMISSION_DENIED) {
			cancel()
			c.invalidateLock() // Clear lock state
			return resp, &InvalidTokenError{Message: resp.ErrorMessage}
		}

		// For other errors, back off and retry
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("server returned status: %v, message: %s", resp.Status, resp.ErrorMessage)
		}

		fmt.Printf("Attempt %d failed: %v, retrying in %v...\n", attempt+1, lastErr, backoff)
		cancel()
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	// All retries failed
	return nil, &ServerUnavailableError{
		Operation: operation,
		Attempts:  maxRetries,
		LastError: lastErr,
	}
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

	// Return the original error to preserve the error type
	return err
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
		// Server unavailable or invalid token errors are already handled by retryRPC
		return err
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
		// For server unavailability during release, we've already invalidated our lock state
		// in the retryRPC function. Just return the error for application handling.
		return err
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

// ServerFencingError is returned when the server is in fencing period
type ServerFencingError struct {
	Operation string
	Message   string
}

func (e *ServerFencingError) Error() string {
	return fmt.Sprintf("operation %s rejected: server is in fencing period - %s", e.Operation, e.Message)
}

// IsServerFencing checks if an error is a ServerFencingError
func IsServerFencing(err error) bool {
	_, ok := err.(*ServerFencingError)
	return ok
}
