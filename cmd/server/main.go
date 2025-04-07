package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"Distributed-Lock-Manager/internal/replication"
	"Distributed-Lock-Manager/internal/server"
	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
)

func main() {
	// Parse command line flags
	serverID := flag.String("id", "", "Server ID")
	serverAddr := flag.String("addr", ":50051", "Server address")
	peerAddrs := flag.String("peers", "", "Comma-separated list of peer addresses (id:host:port,id:host:port)")
	flag.Parse()

	if *serverID == "" {
		log.Fatal("Server ID is required")
	}

	// Parse peer addresses
	var peers []replication.PeerInfo
	if *peerAddrs != "" {
		// Format: id1:host1:port1,id2:host2:port2
		peerList := strings.Split(*peerAddrs, ",")
		for _, peerStr := range peerList {
			parts := strings.Split(peerStr, ":")
			if len(parts) != 3 {
				log.Fatalf("Invalid peer format: %s. Expected format: id:host:port", peerStr)
			}
			peerID := parts[0]
			peerHost := parts[1]
			peerPort := parts[2]
			peerAddr := fmt.Sprintf("%s:%s", peerHost, peerPort)
			peers = append(peers, replication.PeerInfo{
				ID:      peerID,
				Address: peerAddr,
			})
			log.Printf("Added peer: %s at %s", peerID, peerAddr)
		}
	}

	// Create logger
	logger := log.New(os.Stdout, fmt.Sprintf("[SERVER-%s] ", *serverID), log.LstdFlags)

	// Create replication manager
	replConfig := replication.ServerConfig{
		ServerID: *serverID,
		Address:  *serverAddr,
		Peers:    peers,
		ConnConfig: replication.ConnectionConfig{
			RetryAttempts:     3,
			InitialRetryDelay: 100 * time.Millisecond,
			MaxRetryDelay:     1 * time.Second,
			ConnectionTimeout: 2 * time.Second,
			ReconnectInterval: 5 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
	}

	replManager := replication.NewReplicationManager(replConfig, logger)

	// Start replication manager
	if err := replManager.Start(); err != nil {
		logger.Fatalf("Failed to start replication manager: %v", err)
	}
	defer replManager.Stop()

	// Create gRPC server
	lis, err := net.Listen("tcp", *serverAddr)
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	lockServer := server.NewLockServer(*serverID, logger)
	pb.RegisterLockServiceServer(grpcServer, lockServer)

	// Start gRPC server
	logger.Printf("Starting server on %s", *serverAddr)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	logger.Println("Shutting down server...")
	grpcServer.GracefulStop()
	logger.Println("Server stopped")
}
