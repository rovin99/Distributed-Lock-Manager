package replication

import (
	"log"
	"os"
	"testing"
	"time"
)

func TestNewConnectionPool(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := DefaultConnectionConfig()

	pool := NewConnectionPool(config, logger)
	if pool == nil {
		t.Fatal("Expected non-nil ConnectionPool")
	}

	if pool.config != config {
		t.Error("Expected config to match provided config")
	}

	if len(pool.conns) != 0 {
		t.Errorf("Expected empty connections map, got %d connections", len(pool.conns))
	}
}

func TestGetConnection(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := ConnectionConfig{
		RetryAttempts:     1,
		InitialRetryDelay: 100 * time.Millisecond,
		MaxRetryDelay:     1 * time.Second,
		ConnectionTimeout: 100 * time.Millisecond,
		ReconnectInterval: 1 * time.Second,
		IdleTimeout:       5 * time.Second,
	}

	pool := NewConnectionPool(config, logger)

	// Attempt to get connection to non-existent server
	conn, err := pool.GetConnection("localhost:50051")
	if err == nil {
		t.Error("Expected error when getting connection to non-existent server")
	}
	if conn != nil {
		t.Error("Expected nil connection for non-existent server")
	}
}

func TestCloseConnection(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := DefaultConnectionConfig()

	pool := NewConnectionPool(config, logger)

	// Close non-existent connection
	pool.CloseConnection("localhost:50051")

	if len(pool.conns) != 0 {
		t.Errorf("Expected empty connections map, got %d connections", len(pool.conns))
	}
}

func TestCloseAllConnections(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := DefaultConnectionConfig()

	pool := NewConnectionPool(config, logger)

	// Close all connections when pool is empty
	pool.CloseAllConnections()

	if len(pool.conns) != 0 {
		t.Errorf("Expected empty connections map, got %d connections", len(pool.conns))
	}
}

func TestConnectionPoolConcurrency(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	config := ConnectionConfig{
		RetryAttempts:     1,
		InitialRetryDelay: 100 * time.Millisecond,
		MaxRetryDelay:     1 * time.Second,
		ConnectionTimeout: 100 * time.Millisecond,
		ReconnectInterval: 1 * time.Second,
		IdleTimeout:       5 * time.Second,
	}

	pool := NewConnectionPool(config, logger)

	// Test concurrent access to the connection pool
	const numGoroutines = 10
	done := make(chan bool)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()

			// Attempt to get and close connections concurrently
			address := "localhost:50051"
			_, _ = pool.GetConnection(address)
			pool.CloseConnection(address)
			pool.CloseAllConnections()
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify pool state
	if len(pool.conns) != 0 {
		t.Errorf("Expected empty connections map after concurrent access, got %d connections", len(pool.conns))
	}
}
