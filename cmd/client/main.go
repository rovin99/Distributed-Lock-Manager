package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"

	"Distributed-Lock-Manager/internal/client"
)

func main() {
	// Define command-line flag for port
	port := flag.Int("port", 50051, "The server port")
	flag.Parse()

	// Check for required arguments: at least client ID and message
	args := flag.Args()
	if len(args) < 2 {
		log.Fatalf("Usage: %s -port <port> <client_id> <message> [file_name]", flag.CommandLine.Name())
	}

	// Parse client ID
	clientID, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatalf("Invalid client ID: %v", err)
	}

	message := args[1] // Required
	fileName := "file_0"
	if len(args) >= 3 {
		fileName = args[2]
	}

	// Create server address
	serverAddr := fmt.Sprintf("localhost:%d", *port)

	// Create client
	c, err := client.NewLockClient(serverAddr, int32(clientID))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Initialize client
	if err := c.ClientInit(); err != nil {
		log.Fatalf("Failed to initialize client: %v", err)
	}
	fmt.Printf("Client %d initialized successfully\n", clientID)

	// Acquire lock
	if err := c.LockAcquire(); err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}
	fmt.Printf("Client %d acquired lock successfully\n", clientID)

	// Append to file
	content := fmt.Sprintf("%s from client %d\n", message, clientID)
	if err := c.FileAppend(fileName, []byte(content)); err != nil {
		log.Fatalf("Failed to append to file: %v", err)
	}
	fmt.Printf("Client %d appended to file '%s' successfully\n", clientID, fileName)

	// Release lock
	if err := c.LockRelease(); err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}
	fmt.Printf("Client %d released lock successfully\n", clientID)

	fmt.Printf("Client %d closed successfully\n", clientID)
}
