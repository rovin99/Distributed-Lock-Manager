package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command line flags
	serverAddr := flag.String("addr", "localhost:50051", "Server address to connect to")
	flag.Parse()

	// Create logger
	logger := log.New(log.Writer(), "[TEST-CLIENT] ", log.LstdFlags)
	logger.Printf("Connecting to server at %s", *serverAddr)

	// Connect to the server
	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create client
	client := pb.NewLockServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test client initialization
	clientID := int32(1)
	logger.Printf("Initializing client %d", clientID)
	initResp, err := client.ClientInit(ctx, &pb.ClientInitArgs{
		ClientId: clientID,
	})
	if err != nil {
		logger.Fatalf("Failed to initialize client: %v", err)
	}
	logger.Printf("Client initialization response: %v", initResp)

	// Test lock acquisition
	logger.Printf("Attempting to acquire lock")
	lockResp, err := client.LockAcquire(ctx, &pb.LockArgs{
		ClientId:  clientID,
		RequestId: fmt.Sprintf("%d-%x-%d", clientID, time.Now().UnixNano(), 2),
	})
	if err != nil {
		logger.Fatalf("Failed to acquire lock: %v", err)
	}
	logger.Printf("Lock acquisition response: %v", lockResp)

	if lockResp.Status == pb.Status_OK {
		// Test file append
		logger.Printf("Attempting to append to file")
		fileResp, err := client.FileAppend(ctx, &pb.FileArgs{
			ClientId:  clientID,
			Token:     lockResp.Token,
			Filename:  "file_0",
			Content:   []byte("Hello from test client"),
			RequestId: fmt.Sprintf("%d-%x-%d", clientID, time.Now().UnixNano(), 3),
		})
		if err != nil {
			logger.Fatalf("Failed to append to file: %v", err)
		}
		logger.Printf("File append response: %v", fileResp)

		// Test lock release
		logger.Printf("Attempting to release lock")
		releaseResp, err := client.LockRelease(ctx, &pb.LockArgs{
			ClientId:  clientID,
			Token:     lockResp.Token,
			RequestId: fmt.Sprintf("%d-%x-%d", clientID, time.Now().UnixNano(), 4),
		})
		if err != nil {
			logger.Fatalf("Failed to release lock: %v", err)
		}
		logger.Printf("Lock release response: %v", releaseResp)
	}

	logger.Println("Test completed successfully")
}
