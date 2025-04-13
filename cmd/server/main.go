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
	role := flag.String("role", "primary", "Server role: 'primary' or 'secondary'")
	serverID := flag.Int("id", 1, "Server ID (1 for primary, 2+ for secondaries)")
	peerAddress := flag.String("peer", "", "Address of peer server (legacy, single peer)")
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
	} else if *peerAddress != "" {
		// Legacy mode with single peer
		peerAddresses = []string{*peerAddress}
		log.Printf("Legacy mode with single peer: %s", *peerAddress)
	}

	// Determine server role from flags
	var lockServer *server.LockServer
	if *role == "primary" || *role == "secondary" {
		// Convert role string to ServerRole type
		serverRole := server.PrimaryRole
		if *role == "secondary" {
			serverRole = server.SecondaryRole
		}

		// Create replicated server with new configuration function
		if len(peerAddresses) > 0 {
			// Multi-peer configuration
			lockServer = server.NewReplicatedLockServerWithMultiPeers(serverRole, int32(*serverID), peerAddresses, server.DefaultHeartbeatConfig)
			log.Printf("Starting as %s server (ID: %d) with %d peers", *role, *serverID, len(peerAddresses))
		} else {
			// Legacy with single peer
			lockServer = server.NewReplicatedLockServer(serverRole, int32(*serverID), *peerAddress)
			log.Printf("Starting as %s server (ID: %d) with peer: %s", *role, *serverID, *peerAddress)
		}
	} else {
		// Create standalone server (backward compatibility)
		lockServer = server.NewLockServer()
		log.Printf("Starting as standalone server (invalid role: %s)", *role)
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
	if (len(peerAddresses) > 0 || *peerAddress != "") && !*skipVerifications {
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
