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

// ConnectionPool manages a pool of gRPC connections
type ConnectionPool struct {
	mu     sync.RWMutex
	conns  map[string]*grpc.ClientConn
	config ConnectionConfig
	logger *log.Logger
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(config ConnectionConfig, logger *log.Logger) *ConnectionPool {
	return &ConnectionPool{
		conns:  make(map[string]*grpc.ClientConn),
		config: config,
		logger: logger,
	}
}

// GetConnection returns a connection to the specified address, creating one if necessary
func (p *ConnectionPool) GetConnection(address string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, exists := p.conns[address]
	p.mu.RUnlock()

	if exists && conn != nil {
		// Check if connection is still valid
		state := conn.GetState()
		if state == connectivity.Idle || state == connectivity.Ready {
			return conn, nil
		}

		// Connection is not in a good state, close it and create a new one
		p.logger.Printf("Connection to %s is in state %s, creating a new one", address, state)
		p.CloseConnection(address)
	}

	// Create a new connection
	return p.createConnection(address)
}

// createConnection creates a new connection to the specified address
func (p *ConnectionPool) createConnection(address string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check again in case another goroutine created the connection
	if conn, exists := p.conns[address]; exists && conn != nil {
		state := conn.GetState()
		if state == connectivity.Idle || state == connectivity.Ready {
			return conn, nil
		}
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), p.config.ConnectionTimeout)
	defer cancel()

	// Implement exponential backoff retry
	var conn *grpc.ClientConn
	var err error

	for attempt := 0; attempt < p.config.RetryAttempts; attempt++ {
		p.logger.Printf("Creating connection to %s (attempt %d/%d)", address, attempt+1, p.config.RetryAttempts)

		conn, err = grpc.DialContext(ctx, address, grpc.WithInsecure())
		if err != nil {
			p.logger.Printf("Failed to create connection to %s: %v", address, err)
			continue
		}

		// Wait for the connection to be ready
		state := conn.GetState()
		if state == connectivity.Ready {
			p.conns[address] = conn
			p.logger.Printf("Successfully created connection to %s", address)
			return conn, nil
		}

		// If not ready, wait for a short time and check again
		timeout := time.After(p.config.ConnectionTimeout)
		for {
			select {
			case <-timeout:
				conn.Close()
				err = fmt.Errorf("connection timeout waiting for ready state")
				goto retry
			default:
				state = conn.GetState()
				if state == connectivity.Ready {
					p.conns[address] = conn
					p.logger.Printf("Successfully created connection to %s", address)
					return conn, nil
				}
				if state == connectivity.TransientFailure || state == connectivity.Shutdown {
					conn.Close()
					err = fmt.Errorf("connection failed with state %s", state)
					goto retry
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	retry:
		// Calculate delay with exponential backoff
		delay := p.config.InitialRetryDelay * time.Duration(1<<uint(attempt))
		if delay > p.config.MaxRetryDelay {
			delay = p.config.MaxRetryDelay
		}

		p.logger.Printf("Failed to establish connection to %s: %v, retrying in %v", address, err, delay)
		time.Sleep(delay)
	}

	return nil, fmt.Errorf("failed to establish connection to %s after %d attempts: %v", address, p.config.RetryAttempts, err)
}

// monitorConnection monitors the connection and closes it if it's not in a good state
func (p *ConnectionPool) monitorConnection(address string, conn *grpc.ClientConn) {
	ticker := time.NewTicker(p.config.IdleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			state := conn.GetState()
			if state != connectivity.Idle && state != connectivity.Ready {
				p.logger.Printf("Connection to %s is in state %s, closing it", address, state)
				p.CloseConnection(address)
				return
			}
		}
	}
}

// CloseConnection closes the connection to the specified address
func (p *ConnectionPool) CloseConnection(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, exists := p.conns[address]; exists {
		conn.Close()
		delete(p.conns, address)
		p.logger.Printf("Closed connection to %s", address)
	}
}

// CloseAllConnections closes all connections in the pool
func (p *ConnectionPool) CloseAllConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for address, conn := range p.conns {
		conn.Close()
		p.logger.Printf("Closed connection to %s", address)
	}

	p.conns = make(map[string]*grpc.ClientConn)
}
