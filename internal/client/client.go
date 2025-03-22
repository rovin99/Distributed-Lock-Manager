package client

import (
	"context"
	"fmt"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LockClient wraps the gRPC client functionality
type LockClient struct {
	conn   *grpc.ClientConn
	client pb.LockServiceClient
	id     int32
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
		conn:   conn,
		client: client,
		id:     clientID,
	}, nil
}

// Initialize initializes the client with the server
func (c *LockClient) Initialize() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.ClientInit(ctx, &pb.Int{Rc: c.id})
	if err != nil {
		return fmt.Errorf("ClientInit failed: %v", err)
	}
	return nil
}

// AcquireLock attempts to acquire the lock
func (c *LockClient) AcquireLock() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lockArgs := &pb.LockArgs{ClientId: c.id}
	resp, err := c.client.LockAcquire(ctx, lockArgs)
	if err != nil {
		return fmt.Errorf("LockAcquire failed: %v", err)
	}
	if resp.Status != pb.Status_SUCCESS {
		return fmt.Errorf("LockAcquire failed with status: %v", resp.Status)
	}
	return nil
}

// AcquireLockWithRetry attempts to acquire the lock with exponential backoff
func (c *LockClient) AcquireLockWithRetry(maxAttempts int) error {
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		// Attempt to acquire lock
		lockArgs := &pb.LockArgs{ClientId: c.id}
		resp, err := c.client.LockAcquire(ctx, lockArgs)
		cancel()

		if err == nil && resp.Status == pb.Status_SUCCESS {
			return nil
		}

		// Save error for return if all attempts fail
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("failed with status: %v", resp.Status)
		}

		// Exponential backoff with jitter
		backoffTime := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
		if backoffTime > 5*time.Second {
			backoffTime = 5 * time.Second
		}
		time.Sleep(backoffTime)
	}

	return fmt.Errorf("failed to acquire lock after %d attempts: %v", maxAttempts, lastErr)
}

// AppendFile appends data to a file
func (c *LockClient) AppendFile(filename string, content []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fileArgs := &pb.FileArgs{
		Filename: filename,
		Content:  content,
		ClientId: c.id,
	}
	resp, err := c.client.FileAppend(ctx, fileArgs)
	if err != nil {
		return fmt.Errorf("FileAppend failed: %v", err)
	}
	if resp.Status != pb.Status_SUCCESS {
		return fmt.Errorf("FileAppend failed with status: %v", resp.Status)
	}
	return nil
}

// ReleaseLock releases the lock
func (c *LockClient) ReleaseLock() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lockArgs := &pb.LockArgs{ClientId: c.id}
	resp, err := c.client.LockRelease(ctx, lockArgs)
	if err != nil {
		return fmt.Errorf("LockRelease failed: %v", err)
	}
	if resp.Status != pb.Status_SUCCESS {
		return fmt.Errorf("LockRelease failed with status: %v", resp.Status)
	}
	return nil
}

// Close closes the client connection
func (c *LockClient) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.ClientClose(ctx, &pb.Int{Rc: c.id})
	if err != nil {
		return fmt.Errorf("ClientClose failed: %v", err)
	}
	return c.conn.Close()
}
