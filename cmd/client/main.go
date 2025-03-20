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

	// Default values
	clientID := int32(1)
	message := "Hello, World!"

	// Parse remaining command-line arguments if provided
	args := flag.Args()
	if len(args) > 0 {
		id, err := strconv.Atoi(args[0])
		if err == nil {
			clientID = int32(id)
		}
	}
	if len(args) > 1 {
		message = args[1]
	}

	// Create server address using the port
	serverAddr := fmt.Sprintf("localhost:%d", *port)

	// Create a new client with the specified ID and server address
	c, err := client.NewLockClient(serverAddr, clientID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Step 1: Initialize the client
	if err := c.Initialize(); err != nil {
		log.Fatalf("Failed to initialize client: %v", err)
	}
	fmt.Printf("Client %d initialized successfully\n", clientID)

	// Step 2: Acquire the lock
	if err := c.AcquireLock(); err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}
	fmt.Printf("Client %d acquired lock successfully\n", clientID)

	// Step 3: Append data to a file
	content := fmt.Sprintf("%s from client %d\n", message, clientID)
	if err := c.AppendFile("file_0", []byte(content)); err != nil {
		log.Fatalf("Failed to append to file: %v", err)
	}
	fmt.Printf("Client %d appended to file successfully\n", clientID)

	// Step 4: Release the lock
	if err := c.ReleaseLock(); err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}
	fmt.Printf("Client %d released lock successfully\n", clientID)

	fmt.Printf("Client %d closed successfully\n", clientID)
}
