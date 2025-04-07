package replication

import (
	"log"
	"os"
	"testing"
	"time"
)

func TestNewPeer(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	peer := NewPeer("test-peer", "localhost:50051", logger)

	if peer.ID != "test-peer" {
		t.Errorf("Expected peer ID test-peer, got %s", peer.ID)
	}

	if peer.Address != "localhost:50051" {
		t.Errorf("Expected peer address localhost:50051, got %s", peer.Address)
	}

	if peer.State != Disconnected {
		t.Errorf("Expected initial state Disconnected, got %v", peer.State)
	}

	if peer.Conn != nil {
		t.Error("Expected nil connection for new peer")
	}
}

func TestPeerConnect(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	peer := NewPeer("test-peer", "localhost:50051", logger)
	config := ConnectionConfig{
		RetryAttempts:     1,
		InitialRetryDelay: 100 * time.Millisecond,
		MaxRetryDelay:     1 * time.Second,
		ConnectionTimeout: 100 * time.Millisecond,
		ReconnectInterval: 1 * time.Second,
		IdleTimeout:       5 * time.Second,
	}

	// Attempt to connect to non-existent server
	err := peer.Connect(config)
	if err == nil {
		t.Error("Expected error when connecting to non-existent server")
	}

	if peer.State != Disconnected {
		t.Errorf("Expected state Disconnected after failed connection, got %v", peer.State)
	}

	if peer.Conn != nil {
		t.Error("Expected nil connection after failed connection attempt")
	}
}

func TestPeerDisconnect(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	peer := NewPeer("test-peer", "localhost:50051", logger)

	// Disconnect when not connected
	peer.Disconnect()
	if peer.State != Disconnected {
		t.Errorf("Expected state Disconnected after disconnect, got %v", peer.State)
	}

	if peer.Conn != nil {
		t.Error("Expected nil connection after disconnect")
	}
}

func TestPeerIsConnected(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	peer := NewPeer("test-peer", "localhost:50051", logger)

	// Initially not connected
	if peer.IsConnected() {
		t.Error("Expected peer to be initially disconnected")
	}

	// Set state to Connected
	peer.mu.Lock()
	peer.State = Connected
	peer.mu.Unlock()

	if !peer.IsConnected() {
		t.Error("Expected peer to be connected after state change")
	}
}

func TestPeerGetConnectionState(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	peer := NewPeer("test-peer", "localhost:50051", logger)

	// Test initial state
	if state := peer.GetConnectionState(); state != Disconnected {
		t.Errorf("Expected initial state Disconnected, got %v", state)
	}

	// Test state transitions
	peer.mu.Lock()
	peer.State = Connecting
	peer.mu.Unlock()

	if state := peer.GetConnectionState(); state != Connecting {
		t.Errorf("Expected state Connecting, got %v", state)
	}

	peer.mu.Lock()
	peer.State = Connected
	peer.mu.Unlock()

	if state := peer.GetConnectionState(); state != Connected {
		t.Errorf("Expected state Connected, got %v", state)
	}
}
