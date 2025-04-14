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

	fmt.Printf("DEBUG: Client initialized with serverAddrs=%v (lazy connection will be established on first RPC call)\n",
		client.serverAddrs)

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

// retryRPC handles retrying an RPC operation with failover to other servers
func (c *LockClient) retryRPC(operation string, rpcFunc func(context.Context) (*pb.LockResponse, error)) (*pb.LockResponse, error) {
	var lastError error
	var lastResponse *pb.LockResponse
	retries := 0
	backoff := initialBackoff

	// Try the current server first
	for retries < maxRetries {
		if c.client == nil {
			if err := c.connectToCurrentServer(); err != nil {
				fmt.Printf("Failed to connect to current server: %v. Trying next server...\n", err)
				if err := c.tryNextServer(); err != nil {
					return nil, &ServerUnavailableError{
						Operation: operation,
						Attempts:  retries + 1,
						LastError: err,
					}
				}
				continue
			}
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)
		resp, err := rpcFunc(ctx)
		cancel()

		if err == nil {
			// Handle successful RPC with response

			// Check for invalid token first
			if resp.Status == pb.Status_INVALID_TOKEN || resp.Status == pb.Status_PERMISSION_DENIED {
				// Invalid token, clear lock state and return specific error
				c.invalidateLock()
				return resp, &InvalidTokenError{Message: resp.ErrorMessage}
			}

			// Handle SECONDARY_MODE responses with leader redirection
			if resp.Status == pb.Status_SECONDARY_MODE {
				// Check if the error message contains leader address info (e.g., "...current leader: host:port")
				leaderAddrInfo := c.extractLeaderAddress(resp.ErrorMessage)
				if leaderAddrInfo != "" {
					fmt.Printf("Got SECONDARY_MODE with leader info: %s\n", leaderAddrInfo)
					// Try to add this address to our server list if it's not already there
					c.addServerIfMissing(leaderAddrInfo)
					// Try to connect to this server next
					if err := c.tryConnectToServer(leaderAddrInfo); err != nil {
						return nil, err
					}
					continue
				}

				// No leader info, try the next server
				fmt.Printf("Got SECONDARY_MODE without leader info, trying next server\n")
				if err := c.tryNextServer(); err != nil {
					return nil, &ServerUnavailableError{
						Operation: operation,
						Attempts:  retries + 1,
						LastError: fmt.Errorf("all servers in SECONDARY_MODE"),
					}
				}
				continue
			}

			// For SERVER_FENCING, we should retry with exponential backoff
			if resp.Status == pb.Status_SERVER_FENCING {
				fmt.Printf("Server is fencing - retrying in %v...\n", backoff)
				lastResponse = resp
				time.Sleep(backoff)
				backoff *= 2 // exponential backoff
				retries++
				continue
			}

			// Handle ERROR status - treat as a failure that should be retried
			if resp.Status == pb.Status_ERROR {
				fmt.Printf("Server returned ERROR status: %s - retrying in %v...\n", resp.ErrorMessage, backoff)
				lastResponse = resp
				lastError = fmt.Errorf("server returned status: %v, message: %s", resp.Status, resp.ErrorMessage)
				time.Sleep(backoff)
				backoff *= 2 // exponential backoff
				retries++
				continue
			}

			// For other status codes (like OK), return the response
			return resp, nil
		} else {
			// RPC failed with network error
			fmt.Printf("RPC error: %v. Attempting retry...\n", err)
			lastError = err

			// First try retrying on the same server a few times (for transient network issues)
			// Only switch servers after 2 consecutive failures on the same server
			if retries%2 == 1 {
				// After two failed attempts on same server, try another
				fmt.Printf("Multiple failures on current server, trying next server...\n")
				if err := c.tryNextServer(); err != nil {
					return nil, &ServerUnavailableError{
						Operation: operation,
						Attempts:  retries + 1,
						LastError: err,
					}
				}
			} else {
				// For first failure, just back off and retry same server
				time.Sleep(backoff)
				backoff *= 2 // exponential backoff
			}

			retries++
			continue
		}
	}

	// If we have a SERVER_FENCING response and exhausted retries
	if lastResponse != nil && lastResponse.Status == pb.Status_SERVER_FENCING {
		return nil, &ServerFencingError{
			Operation: operation,
			Message:   lastResponse.ErrorMessage,
		}
	}

	// If we have an ERROR response and exhausted retries
	if lastResponse != nil && lastResponse.Status == pb.Status_ERROR {
		return nil, &ServerUnavailableError{
			Operation: operation,
			Attempts:  retries,
			LastError: fmt.Errorf("server returned ERROR: %s", lastResponse.ErrorMessage),
		}
	}

	// We exhausted all retries
	c.invalidateLock() // Reset lock state on persistent errors
	return nil, &ServerUnavailableError{
		Operation: operation,
		Attempts:  retries,
		LastError: lastError,
	}
}

// extractLeaderAddress parses an error message to extract leader address information
// Example: "Server is not the leader (current leader: 10.0.0.1:50051)"
func (c *LockClient) extractLeaderAddress(errMsg string) string {
	if strings.Contains(errMsg, "current leader:") {
		parts := strings.Split(errMsg, "current leader:")
		if len(parts) >= 2 {
			return strings.TrimSpace(parts[1])
		}
	}
	return ""
}

// addServerIfMissing adds a server address to our list if not already present
func (c *LockClient) addServerIfMissing(serverAddr string) {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()

	// Check if the address is already in our list
	for _, addr := range c.serverAddrs {
		if addr == serverAddr {
			return // Already in the list
		}
	}

	// Add to our list
	c.serverAddrs = append(c.serverAddrs, serverAddr)
	fmt.Printf("Added new server address to failover list: %s\n", serverAddr)
}

// tryConnectToServer attempts to connect to a specific server
func (c *LockClient) tryConnectToServer(serverAddr string) error {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()

	// Close existing connection if there is one
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.client = nil
	}

	fmt.Printf("Attempting to connect to server %s\n", serverAddr)
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("grpc.Dial(%s) failed with error: %v\n", serverAddr, err)
		return fmt.Errorf("failed to connect to server %s: %w", serverAddr, err)
	}

	c.conn = conn
	c.client = pb.NewLockServiceClient(conn)

	// Update the currentServerIdx to match this server if it's in our list
	for i, addr := range c.serverAddrs {
		if addr == serverAddr {
			c.currentServerIdx = i
			break
		}
	}

	fmt.Printf("Successfully connected to server at %s\n", serverAddr)
	return nil
}

// retryLeaseRenewal is similar to retryRPC but for lease renewal operations
func (c *LockClient) retryLeaseRenewal(operation string, rpcFunc func(context.Context) (*pb.LeaseResponse, error)) (*pb.LeaseResponse, error) {
	var lastErr error
	backoff := initialBackoff

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check if we need to establish initial connection
		if c.client == nil {
			fmt.Printf("No active connection for %s operation, establishing connection first...\n", operation)
			if err := c.connectToCurrentServer(); err != nil {
				// If first server fails, try others
				if attemptErr := c.tryNextServer(); attemptErr != nil {
					return nil, &ServerUnavailableError{
						Operation: operation,
						Attempts:  1,
						LastError: fmt.Errorf("failed to establish initial connection: %w", attemptErr),
					}
				}
			}
		}

		// Create a context with timeout for this attempt
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		// Execute the RPC
		resp, err := rpcFunc(ctx)

		// Check for success
		if err == nil && resp.Status == pb.Status_OK {
			cancel() // Remember to cancel the context on success
			return resp, nil
		}

		// Handle token validation errors - do this first for consistency
		if err == nil && (resp.Status == pb.Status_INVALID_TOKEN || resp.Status == pb.Status_PERMISSION_DENIED) {
			cancel()
			c.invalidateLock() // Clear lock state
			return resp, &InvalidTokenError{Message: resp.ErrorMessage}
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
		if err != nil {
			fmt.Printf("Error on attempt %d: %v\n", attempt+1, err)
			cancel()

			// First try retrying on the same server a few times (for transient network issues)
			// Only switch servers after 2 consecutive failures on the same server
			if attempt%2 == 1 && (strings.Contains(err.Error(), "connection") ||
				strings.Contains(err.Error(), "Unavailable") ||
				strings.Contains(err.Error(), "DeadlineExceeded")) {
				// After two failed attempts on same server, try another
				fmt.Printf("Multiple connection failures on current server, trying next server...\n")
				if reconnErr := c.tryNextServer(); reconnErr != nil {
					lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
					break
				}
				continue
			} else {
				// Just back off and retry same server
				lastErr = err
				time.Sleep(backoff)
				backoff *= 2 // Exponential backoff
				continue
			}
		}

		// For other response errors, back off and retry
		lastErr = fmt.Errorf("server returned status: %v, message: %s", resp.Status, resp.ErrorMessage)
		fmt.Printf("Attempt %d failed: %v, retrying in %v...\n", attempt+1, lastErr, backoff)
		cancel()
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	// All retries failed
	c.invalidateLock() // Reset lock state on persistent errors
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
		// Check if we need to establish initial connection
		if c.client == nil {
			fmt.Printf("No active connection for %s operation, establishing connection first...\n", operation)
			if err := c.connectToCurrentServer(); err != nil {
				// If first server fails, try others
				if attemptErr := c.tryNextServer(); attemptErr != nil {
					return nil, &ServerUnavailableError{
						Operation: operation,
						Attempts:  1,
						LastError: fmt.Errorf("failed to establish initial connection: %w", attemptErr),
					}
				}
			}
		}

		// Create a context with timeout for this attempt
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)

		// Execute the RPC
		resp, err := rpcFunc(ctx)

		// Check for success
		if err == nil && resp.Status == pb.Status_OK {
			cancel() // Remember to cancel the context on success
			return resp, nil
		}

		// Handle token validation errors - do this first for consistency
		if err == nil && (resp.Status == pb.Status_INVALID_TOKEN || resp.Status == pb.Status_PERMISSION_DENIED) {
			cancel()
			c.invalidateLock() // Clear lock state
			return resp, &InvalidTokenError{Message: resp.ErrorMessage}
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
		if err != nil {
			fmt.Printf("Error on attempt %d: %v\n", attempt+1, err)
			cancel()

			// First try retrying on the same server a few times (for transient network issues)
			// Only switch servers after 2 consecutive failures on the same server
			if attempt%2 == 1 && (strings.Contains(err.Error(), "connection") ||
				strings.Contains(err.Error(), "Unavailable") ||
				strings.Contains(err.Error(), "DeadlineExceeded")) {
				// After two failed attempts on same server, try another
				fmt.Printf("Multiple connection failures on current server, trying next server...\n")
				if reconnErr := c.tryNextServer(); reconnErr != nil {
					lastErr = fmt.Errorf("failed to connect to another server: %w", reconnErr)
					break
				}
				continue
			} else {
				// Just back off and retry same server
				lastErr = err
				time.Sleep(backoff)
				backoff *= 2 // Exponential backoff
				continue
			}
		}

		// For other response errors, back off and retry
		lastErr = fmt.Errorf("server returned status: %v, message: %s", resp.Status, resp.ErrorMessage)
		fmt.Printf("Attempt %d failed: %v, retrying in %v...\n", attempt+1, lastErr, backoff)
		cancel()
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	// All retries failed
	c.invalidateLock() // Reset lock state on persistent errors
	return nil, &ServerUnavailableError{
		Operation: operation,
		Attempts:  maxRetries,
		LastError: lastErr,
	}
}

// renewLease sends a lease renewal request to the server
func (c *LockClient) renewLease(token string) error {
	// Generate a single request ID for the entire operation including retries
	requestID := c.GenerateRequestID()

	args := &pb.LeaseArgs{
		ClientId:  c.id,
		Token:     token,
		RequestId: requestID,
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
	// Generate a single request ID for the entire operation including retries
	requestID := c.GenerateRequestID()

	args := &pb.FileArgs{
		ClientId:  c.id,
		Filename:  filename,
		Content:   content,
		Token:     c.lockToken,
		RequestId: requestID,
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
	// Generate a single request ID for the entire operation including retries
	requestID := c.GenerateRequestID()

	args := &pb.ClientInitArgs{
		ClientId:  c.id,
		RequestId: requestID,
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
	// Generate a single request ID for the entire operation including retries
	requestID := c.GenerateRequestID()

	args := &pb.LockArgs{
		ClientId:  c.id,
		RequestId: requestID,
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
	// Generate a single request ID for the entire operation including retries
	requestID := c.GenerateRequestID()

	args := &pb.LockArgs{
		ClientId:  c.id,
		Token:     c.lockToken,
		RequestId: requestID,
	}

	fmt.Printf("DEBUG: LockRelease sending request with token: '%s'\n", args.Token)

	_, err := c.retryRPC("LockRelease", func(ctx context.Context) (*pb.LockResponse, error) {
		return c.client.LockRelease(ctx, args)
	})

	if err != nil {
		// Server unavailability or invalid token errors are already handled by retryRPC
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

// ServerUnavailableError is returned when all servers are unreachable
type ServerUnavailableError struct {
	Operation string
	Attempts  int
	LastError error
}

func (e *ServerUnavailableError) Error() string {
	return fmt.Sprintf("operation %s failed after %d attempts: %v", e.Operation, e.Attempts, e.LastError)
}

// IsServerUnavailable checks if an error is a ServerUnavailableError
func IsServerUnavailable(err error) bool {
	_, ok := err.(*ServerUnavailableError)
	return ok
}

// GetServerInfo retrieves information about the server
func (c *LockClient) GetServerInfo() (map[string]interface{}, error) {
	// Ensure we have a connection
	if c.client == nil {
		if err := c.connectToCurrentServer(); err != nil {
			return nil, fmt.Errorf("failed to connect to server: %v", err)
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)
	defer cancel()

	// Call ServerInfo RPC
	resp, err := c.client.ServerInfo(ctx, &pb.ServerInfoRequest{})
	if err != nil {
		// Try next server on connection error
		if reconnErr := c.tryNextServer(); reconnErr != nil {
			return nil, fmt.Errorf("failed to get server info: %v, reconnect error: %v", err, reconnErr)
		}

		// Try again with new server
		ctx, cancel := context.WithTimeout(context.Background(), initialTimeout)
		resp, err = c.client.ServerInfo(ctx, &pb.ServerInfoRequest{})
		cancel()

		if err != nil {
			return nil, fmt.Errorf("failed to get server info after server switch: %v", err)
		}
	}

	// Create a map to return
	info := map[string]interface{}{
		"server_id":      resp.ServerId,
		"role":           resp.Role,
		"current_epoch":  resp.CurrentEpoch,
		"leader_address": "", // May be filled later if available in the response
	}

	// Add leader address if available (depends on your proto definition)
	if field, ok := any(resp).(interface{ GetLeaderAddress() string }); ok {
		leader := field.GetLeaderAddress()
		if leader != "" {
			info["leader_address"] = leader
		}
	}

	return info, nil
}
