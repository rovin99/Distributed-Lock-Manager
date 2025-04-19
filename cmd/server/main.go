package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"Distributed-Lock-Manager/internal/server"
	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
)

func main() {
	// Define flags
	address := flag.String("address", ":50051", "Address to listen on")
	recoveryTimeout := flag.Duration("recovery-timeout", 30*time.Second, "Timeout for WAL recovery")
	metricsAddress := flag.String("metrics-address", ":8080", "Address for metrics server (set to empty to disable)")

	// Add replication flags - Modified for Phase 1
	serverID := flag.Int("id", 1, "Server ID (1, 2, 3, ...)")
	servers := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "Comma-separated list of all server addresses (ID 1, ID 2, ID 3, ...)")
	skipVerifications := flag.Bool("skip-verifications", false, "Skip ID and filesystem verifications")

	flag.Parse()

	// Initialize the files
	server.CreateFiles()

	// Create gRPC server
	s := grpc.NewServer()

	// Parse server addresses
	serverAddressesList := strings.Split(*servers, ",")
	if len(serverAddressesList) < 3 {
		log.Fatalf("Error: At least 3 server addresses required in -servers flag")
	}

	// Create a map of server IDs to addresses
	allServerAddresses := make(map[int32]string)
	// Assuming the list order corresponds to IDs 1, 2, 3...
	for i, addr := range serverAddressesList {
		allServerAddresses[int32(i+1)] = addr
	}

	clusterSize := len(allServerAddresses)

	// Determine the initial role based on server ID
	initialPrimaryID := int32(1) // Assume lowest ID starts as primary
	initialRole := server.SecondaryRole
	if int32(*serverID) == initialPrimaryID {
		initialRole = server.PrimaryRole
	}

	// Create the replicated lock server with the new parameters
	lockServer := server.NewReplicatedLockServer(
		initialRole,
		int32(*serverID),
		allServerAddresses, // Pass the map
		clusterSize,        // Pass the size
		initialPrimaryID,   // Pass who starts as primary
	)

	log.Printf("Starting Server ID %d as %s (Initial Primary: %d, Cluster Size: %d)",
		*serverID, initialRole, initialPrimaryID, clusterSize)

	pb.RegisterLockServiceServer(s, lockServer)

	// Start metrics server if address is provided
	if *metricsAddress != "" {
		log.Printf("Starting metrics server at %s", *metricsAddress)
		lockServer.GetMetrics().StartMetricsServer(*metricsAddress)
	}

	// Wait for WAL recovery to complete
	log.Printf("Waiting for WAL recovery to complete...")
	if err := lockServer.WaitForRecovery(*recoveryTimeout); err != nil {
		log.Printf("Warning: WAL recovery completed with issues: %v", err)
		log.Printf("Server will start but may have inconsistent state")
	} else {
		log.Printf("WAL recovery completed successfully")
	}

	// Perform verifications if not explicitly skipped
	if !*skipVerifications {
		// Run the verification functions that have been updated to work with multiple peers
		log.Printf("Verifying server ID uniqueness...")
		if err := lockServer.VerifyUniqueServerID(); err != nil {
			log.Fatalf("Server ID verification failed: %v", err)
		}
		log.Printf("Server ID verification completed successfully")

		log.Printf("Verifying shared filesystem access...")
		if err := lockServer.VerifySharedFilesystem(); err != nil {
			log.Fatalf("Shared filesystem verification failed: %v", err)
		}
		log.Printf("Shared filesystem verification completed successfully")
	} else {
		log.Printf("Skipping server ID and shared filesystem verifications (use -skip-verifications=false to enable)")
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
