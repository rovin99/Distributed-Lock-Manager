package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"Distributed-Lock-Manager/internal/server"
	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// HTTP monitoring port (derived from server ID)
	httpPort := flag.Int("http-port", 0, "Port for HTTP monitoring (if 0, uses 8081 + server ID)")

	flag.Parse()

	// If HTTP port not explicitly set, derive it from server ID
	if *httpPort == 0 {
		*httpPort = 8081 + *serverID - 1 // ServerID 1 -> 8081, ServerID 2 -> 8082, etc.
	}

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

	// Check existing servers to detect running primary
	var existingPrimaryID int32 = -1
	for id, addr := range allServerAddresses {
		if int32(*serverID) == id {
			continue // Skip self
		}

		// Try to connect to the server to see if it's running
		dialCtx, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancelDial()

		if err == nil {
			// Successfully connected, check its role
			client := pb.NewLockServiceClient(conn)
			infoCtx, cancelInfo := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := client.ServerInfo(infoCtx, &pb.ServerInfoRequest{})
			cancelInfo()

			if err == nil && resp.Role == "primary" {
				log.Printf("Detected server ID %d as the current PRIMARY", id)
				existingPrimaryID = id
				conn.Close()
				break
			}

			conn.Close()
		}
	}

	// Determine the initial role based on server ID and detected primary
	initialPrimaryID := int32(1) // Default assumption
	if existingPrimaryID != -1 {
		initialPrimaryID = existingPrimaryID
	}

	initialRole := server.SecondaryRole
	if int32(*serverID) == initialPrimaryID && existingPrimaryID == -1 {
		// Only become primary if we're the default AND no other primary was detected
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

	// Start HTTP monitoring server
	lockServer.StartHTTPMonitoring(*httpPort)
	log.Printf("HTTP monitoring available at http://localhost:%d/", *httpPort)

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
			log.Printf("WARNING: Server ID verification failed: %v", err)
			// Not fatal, continue startup
		} else {
			log.Printf("Server ID verification completed successfully")
		}

		log.Printf("Verifying shared filesystem access...")
		if err := lockServer.VerifySharedFilesystem(); err != nil {
			log.Printf("WARNING: Shared filesystem verification failed: %v", err)
			// Not fatal, continue startup
		} else {
			log.Printf("Shared filesystem verification completed successfully")
		}
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
