package replication

import (
	"time"
)

// ServerConfig holds the configuration for a server
type ServerConfig struct {
	ServerID   string
	Address    string
	Peers      []PeerInfo
	ConnConfig ConnectionConfig
}

// PeerInfo represents information about a peer server
type PeerInfo struct {
	ID      string
	Address string
}

// ConnectionConfig holds configuration for peer connections
type ConnectionConfig struct {
	// RetryAttempts is the maximum number of connection attempts
	RetryAttempts int
	// InitialRetryDelay is the initial delay between retry attempts
	InitialRetryDelay time.Duration
	// MaxRetryDelay is the maximum delay between retry attempts
	MaxRetryDelay time.Duration
	// ReconnectInterval is the interval for periodic reconnection attempts
	ReconnectInterval time.Duration
	// ConnectionTimeout is the timeout for establishing a connection
	ConnectionTimeout time.Duration
	// IdleTimeout is the timeout for idle connections
	IdleTimeout time.Duration
}

// DefaultConnectionConfig returns a default connection configuration
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		RetryAttempts:     3,
		InitialRetryDelay: 1 * time.Second,
		MaxRetryDelay:     10 * time.Second,
		ReconnectInterval: 5 * time.Second,
		ConnectionTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
}

// NewServerConfig creates a new server configuration with default connection settings
func NewServerConfig(serverID, address string, peers []PeerInfo) *ServerConfig {
	return &ServerConfig{
		ServerID:   serverID,
		Address:    address,
		Peers:      peers,
		ConnConfig: DefaultConnectionConfig(),
	}
}
