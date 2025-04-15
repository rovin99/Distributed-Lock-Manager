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

	// Add replication flags
	serverID := flag.Int("id", 1, "Server ID (1 for primary, 2+ for secondaries)")
	peers := flag.String("peers", "", "Comma-separated list of all peer server addresses (e.g., 'host2:port,host3:port')")
	skipVerifications := flag.Bool("skip-verifications", false, "Skip ID and filesystem verifications")

	flag.Parse()

	// Initialize the files
	server.CreateFiles()

	// Create gRPC server
	s := grpc.NewServer()

	// Process peer addresses
	var peerAddresses []string
	if *peers != "" {
		peerAddresses = strings.Split(*peers, ",")
		log.Printf("Configured with %d peers: %v", len(peerAddresses), peerAddresses)
	}

	// Create lock server
	var lockServer *server.LockServer
	if len(peerAddresses) > 0 {
		log.Printf("Starting replicated lock server with ID %d and peers: %v", *serverID, peerAddresses)
		lockServer = server.NewReplicatedLockServerWithMultiPeers(int32(*serverID), *address, peerAddresses, server.DefaultHeartbeatConfig)
	} else {
		log.Printf("Starting standalone lock server with ID %d", *serverID)
		lockServer = server.NewReplicatedLockServerWithMultiPeers(int32(*serverID), *address, []string{}, server.DefaultHeartbeatConfig)
	}

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

	// Perform verifications if this is a replicated setup
	if len(peerAddresses) > 0 && !*skipVerifications {
		// Verify server ID uniqueness
		log.Printf("Verifying server ID uniqueness...")
		if err := lockServer.VerifyUniqueServerID(); err != nil {
			log.Fatalf("Server ID verification failed: %v", err)
		}
		log.Printf("Server ID verification completed")

		// Verify shared filesystem
		log.Printf("Verifying shared filesystem access...")
		if err := lockServer.VerifySharedFilesystem(); err != nil {
			log.Fatalf("Shared filesystem verification failed: %v", err)
		}
		log.Printf("Shared filesystem verification completed")
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
