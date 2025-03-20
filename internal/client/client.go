package client

import (
	"context"
	"fmt"

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
	_, err := c.client.ClientInit(context.Background(), &pb.Int{Rc: 0})
	if err != nil {
		return fmt.Errorf("ClientInit failed: %v", err)
	}
	return nil
}

// AcquireLock attempts to acquire the lock
func (c *LockClient) AcquireLock() error {
	lockArgs := &pb.LockArgs{ClientId: c.id}
	resp, err := c.client.LockAcquire(context.Background(), lockArgs)
	if err != nil {
		return fmt.Errorf("LockAcquire failed: %v", err)
	}
	if resp.Status != pb.Status_SUCCESS {
		return fmt.Errorf("LockAcquire failed with status: %v", resp.Status)
	}
	return nil
}

// AppendFile appends data to a file
func (c *LockClient) AppendFile(filename string, content []byte) error {
	fileArgs := &pb.FileArgs{
		Filename: filename,
		Content:  content,
		ClientId: c.id,
	}
	resp, err := c.client.FileAppend(context.Background(), fileArgs)
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
	lockArgs := &pb.LockArgs{ClientId: c.id}
	resp, err := c.client.LockRelease(context.Background(), lockArgs)
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
	_, err := c.client.ClientClose(context.Background(), &pb.Int{Rc: 0})
	if err != nil {
		return fmt.Errorf("ClientClose failed: %v", err)
	}
	return c.conn.Close()
}
