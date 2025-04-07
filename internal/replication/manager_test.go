package replication

import (
	"log"
	"os"
	"testing"
	"time"
)

func TestNewReplicationManager(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := ServerConfig{
		ServerID: "test-server-1",
		Address:  "localhost:50051",
		Peers: []PeerInfo{
			{ID: "test-server-2", Address: "localhost:50052"},
			{ID: "test-server-3", Address: "localhost:50053"},
		},
		ConnConfig: DefaultConnectionConfig(),
	}

	manager := NewReplicationManager(config, logger)
	if manager == nil {
		t.Fatal("Expected non-nil ReplicationManager")
	}

	if manager.config.ServerID != config.ServerID {
		t.Errorf("Expected ServerID %s, got %s", config.ServerID, manager.config.ServerID)
	}

	if len(manager.peers) != 0 {
		t.Errorf("Expected empty peers map, got %d peers", len(manager.peers))
	}
}

func TestReplicationManagerStartStop(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := ServerConfig{
		ServerID: "test-server-1",
		Address:  "localhost:50051",
		Peers: []PeerInfo{
			{ID: "test-server-2", Address: "localhost:50052"},
			{ID: "test-server-3", Address: "localhost:50053"},
		},
		ConnConfig: ConnectionConfig{
			RetryAttempts:     1,
			InitialRetryDelay: 100 * time.Millisecond,
			MaxRetryDelay:     1 * time.Second,
			ConnectionTimeout: 100 * time.Millisecond,
			ReconnectInterval: 1 * time.Second,
			IdleTimeout:       5 * time.Second,
		},
	}

	manager := NewReplicationManager(config, logger)

	// Start the manager
	err := manager.Start()
	if err != nil {
		t.Fatalf("Failed to start replication manager: %v", err)
	}

	// Verify that peers were initialized
	total, connected := manager.GetPeerCount()
	if total != 2 {
		t.Errorf("Expected 2 total peers, got %d", total)
	}
	if connected != 0 {
		t.Errorf("Expected 0 connected peers (since no servers are running), got %d", connected)
	}

	// Stop the manager
	manager.Stop()

	// Verify that all peers are disconnected
	total, connected = manager.GetPeerCount()
	if total != 2 {
		t.Errorf("Expected 2 total peers after stop, got %d", total)
	}
	if connected != 0 {
		t.Errorf("Expected 0 connected peers after stop, got %d", connected)
	}
}

func TestGetPeer(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := ServerConfig{
		ServerID: "test-server-1",
		Address:  "localhost:50051",
		Peers: []PeerInfo{
			{ID: "test-server-2", Address: "localhost:50052"},
			{ID: "test-server-3", Address: "localhost:50053"},
		},
		ConnConfig: DefaultConnectionConfig(),
	}

	manager := NewReplicationManager(config, logger)
	manager.Start()
	defer manager.Stop()

	// Test getting existing peer
	peer, exists := manager.GetPeer("test-server-2")
	if !exists {
		t.Error("Expected to find peer test-server-2")
	}
	if peer.ID != "test-server-2" {
		t.Errorf("Expected peer ID test-server-2, got %s", peer.ID)
	}

	// Test getting non-existent peer
	peer, exists = manager.GetPeer("non-existent")
	if exists {
		t.Error("Expected not to find non-existent peer")
	}
	if peer != nil {
		t.Error("Expected nil peer for non-existent peer")
	}
}

func TestGetConnectedPeers(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := ServerConfig{
		ServerID: "test-server-1",
		Address:  "localhost:50051",
		Peers: []PeerInfo{
			{ID: "test-server-2", Address: "localhost:50052"},
			{ID: "test-server-3", Address: "localhost:50053"},
		},
		ConnConfig: DefaultConnectionConfig(),
	}

	manager := NewReplicationManager(config, logger)
	manager.Start()
	defer manager.Stop()

	// Initially, no peers should be connected (since no servers are running)
	connectedPeers := manager.GetConnectedPeers()
	if len(connectedPeers) != 0 {
		t.Errorf("Expected 0 connected peers, got %d", len(connectedPeers))
	}
}
