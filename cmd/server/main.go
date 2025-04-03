package main

import (
	"flag"
	"log"
	"net"
	"time"

	"Distributed-Lock-Manager/internal/server"
	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
)

func main() {
	// Define flags
	address := flag.String("address", ":50051", "Address to listen on")
	recoveryTimeout := flag.Duration("recovery-timeout", 30*time.Second, "Timeout for WAL recovery")
	flag.Parse()

	// Initialize the files
	server.CreateFiles()

	// Create gRPC server
	s := grpc.NewServer()
	lockServer := server.NewLockServer()
	pb.RegisterLockServiceServer(s, lockServer)

	// Wait for WAL recovery to complete
	log.Printf("Waiting for WAL recovery to complete...")
	if err := lockServer.WaitForRecovery(*recoveryTimeout); err != nil {
		log.Printf("Warning: WAL recovery completed with issues: %v", err)
		log.Printf("Server will start but may have inconsistent state")
	} else {
		log.Printf("WAL recovery completed successfully")
	}

	// Set up TCP listener using the specified address
	lis, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *address, err)
	}

	// Log the address the server is listening on
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
