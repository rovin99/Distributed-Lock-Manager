package replication

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// ConnectionState represents the state of a peer connection
type ConnectionState int

const (
	// Disconnected means the peer is not connected
	Disconnected ConnectionState = iota
	// Connecting means a connection attempt is in progress
	Connecting
	// Connected means the peer is connected
	Connected
)

// String returns a string representation of the connection state
func (s ConnectionState) String() string {
	switch s {
	case Disconnected:
		return "Disconnected"
	case Connecting:
		return "Connecting"
	case Connected:
		return "Connected"
	default:
		return "Unknown"
	}
}

// Peer represents a connection to a peer server
type Peer struct {
	ID              string
	Address         string
	Conn            *grpc.ClientConn
	Client          interface{} // Will be set to the appropriate gRPC client
	State           ConnectionState
	mu              sync.RWMutex
	logger          *log.Logger
	lastError       error     // Track the last error that occurred
	lastConnectTime time.Time // Track when the last connection was established
}

// NewPeer creates a new peer connection
func NewPeer(id, address string, logger *log.Logger) *Peer {
	return &Peer{
		ID:      id,
		Address: address,
		State:   Disconnected,
		logger:  logger,
	}
}

// Connect establishes a connection to the peer
func (p *Peer) Connect(config ConnectionConfig) error {
	p.mu.Lock()
	if p.State == Connected {
		p.mu.Unlock()
		p.logger.Printf("Already connected to peer %s", p.ID)
		return nil
	}
	p.State = Connecting
	p.mu.Unlock()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectionTimeout)
	defer cancel()

	// Implement exponential backoff retry
	var conn *grpc.ClientConn
	var err error

	for attempt := 0; attempt < config.RetryAttempts; attempt++ {
		p.logger.Printf("Connecting to peer %s (attempt %d/%d)", p.ID, attempt+1, config.RetryAttempts)

		conn, err = grpc.DialContext(ctx, p.Address, grpc.WithInsecure())
		if err != nil {
			p.logger.Printf("Failed to create connection to peer %s: %v", p.ID, err)
			p.lastError = err
			continue
		}

		// Wait for the connection to be ready
		state := conn.GetState()
		if state == connectivity.Ready {
			p.mu.Lock()
			p.Conn = conn
			p.State = Connected
			p.lastConnectTime = time.Now()
			p.lastError = nil
			p.mu.Unlock()
			p.logger.Printf("Successfully connected to peer %s", p.ID)
			return nil
		}

		// If not ready, wait for a short time and check again
		timeout := time.After(config.ConnectionTimeout)
		for {
			select {
			case <-timeout:
				conn.Close()
				err = fmt.Errorf("connection timeout waiting for ready state")
				p.lastError = err
				goto retry
			default:
				state = conn.GetState()
				if state == connectivity.Ready {
					p.mu.Lock()
					p.Conn = conn
					p.State = Connected
					p.lastConnectTime = time.Now()
					p.lastError = nil
					p.mu.Unlock()
					p.logger.Printf("Successfully connected to peer %s", p.ID)
					return nil
				}
				if state == connectivity.TransientFailure || state == connectivity.Shutdown {
					conn.Close()
					err = fmt.Errorf("connection failed with state %s", state)
					p.lastError = err
					goto retry
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	retry:
		// Calculate delay with exponential backoff
		delay := config.InitialRetryDelay * time.Duration(1<<uint(attempt))
		if delay > config.MaxRetryDelay {
			delay = config.MaxRetryDelay
		}

		p.logger.Printf("Failed to establish connection to peer %s: %v, retrying in %v", p.ID, err, delay)
		time.Sleep(delay)
	}

	p.mu.Lock()
	p.State = Disconnected
	p.mu.Unlock()
	return fmt.Errorf("failed to establish connection to peer %s after %d attempts: %v", p.ID, config.RetryAttempts, err)
}

// Disconnect closes the connection to the peer
func (p *Peer) Disconnect() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Conn != nil {
		p.Conn.Close()
		p.Conn = nil
	}

	p.State = Disconnected
	p.logger.Printf("Disconnected from peer %s", p.ID)
}

// IsConnected returns whether the peer is connected
func (p *Peer) IsConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.State == Connected
}

// GetConnectionState returns the current connection state
func (p *Peer) GetConnectionState() ConnectionState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.State
}

// GetLastError returns the last error that occurred during connection
func (p *Peer) GetLastError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastError
}

// GetLastConnectTime returns the time when the last connection was established
func (p *Peer) GetLastConnectTime() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastConnectTime
}

// GetConnectionInfo returns information about the peer connection
func (p *Peer) GetConnectionInfo() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var lastErrorStr string
	if p.lastError != nil {
		lastErrorStr = p.lastError.Error()
	} else {
		lastErrorStr = "none"
	}

	var lastConnectTimeStr string
	if p.lastConnectTime.IsZero() {
		lastConnectTimeStr = "never"
	} else {
		lastConnectTimeStr = p.lastConnectTime.Format(time.RFC3339)
	}

	return fmt.Sprintf("Peer %s at %s: State=%s, LastError=%s, LastConnectTime=%s",
		p.ID, p.Address, p.State.String(), lastErrorStr, lastConnectTimeStr)
}
