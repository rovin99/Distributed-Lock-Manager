package replication

import (
	"context"
	"log"
	"sync"
	"time"
)

// ReplicationManager manages the replication state and peer connections
type ReplicationManager struct {
	config     ServerConfig
	peers      map[string]*Peer
	connPool   *ConnectionPool
	logger     *log.Logger
	mu         sync.RWMutex
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(config ServerConfig, logger *log.Logger) *ReplicationManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &ReplicationManager{
		config:     config,
		peers:      make(map[string]*Peer),
		connPool:   NewConnectionPool(config.ConnConfig, logger),
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

// Start initializes the replication manager and begins peer discovery
func (m *ReplicationManager) Start() error {
	m.logger.Printf("Starting replication manager for server %s", m.config.ServerID)

	// Initialize connections to all configured peers
	for _, peerInfo := range m.config.Peers {
		if peerInfo.ID == m.config.ServerID {
			m.logger.Printf("Skipping self connection to %s", peerInfo.ID)
			continue // Skip self
		}

		peer := NewPeer(peerInfo.ID, peerInfo.Address, m.logger)
		m.peers[peerInfo.ID] = peer
		m.logger.Printf("Added peer %s at %s", peerInfo.ID, peerInfo.Address)

		// Attempt to connect to the peer
		if err := peer.Connect(m.config.ConnConfig); err != nil {
			m.logger.Printf("Failed to connect to peer %s: %v", peerInfo.ID, err)
			continue
		}

		m.logger.Printf("Successfully connected to peer %s", peerInfo.ID)
	}

	// Start background goroutine for periodic reconnection attempts
	go m.runReconnectionLoop()

	// Start health check goroutine
	go m.runHealthCheck()

	// Log initial connection status
	total, connected := m.GetPeerCount()
	m.logger.Printf("Replication manager started with %d/%d peers connected", connected, total)

	return nil
}

// Stop shuts down the replication manager and closes all connections
func (m *ReplicationManager) Stop() {
	m.logger.Printf("Stopping replication manager for server %s", m.config.ServerID)

	// Cancel context to stop background goroutines
	m.cancelFunc()

	// Close all peer connections
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, peer := range m.peers {
		peer.Disconnect()
	}

	// Close all connections in the pool
	m.connPool.CloseAllConnections()

	m.logger.Printf("Replication manager stopped")
}

// runReconnectionLoop periodically attempts to reconnect to disconnected peers
func (m *ReplicationManager) runReconnectionLoop() {
	ticker := time.NewTicker(m.config.ConnConfig.ReconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			disconnectedCount := 0
			for _, peer := range m.peers {
				if !peer.IsConnected() {
					disconnectedCount++
				}
			}
			m.mu.RUnlock()

			if disconnectedCount > 0 {
				m.logger.Printf("Found %d disconnected peers, attempting reconnection", disconnectedCount)
			}

			m.mu.RLock()
			for _, peer := range m.peers {
				if !peer.IsConnected() {
					m.logger.Printf("Attempting to reconnect to peer %s", peer.ID)

					// Attempt reconnection in a separate goroutine to avoid blocking
					go func(p *Peer) {
						if err := p.Connect(m.config.ConnConfig); err != nil {
							m.logger.Printf("Failed to reconnect to peer %s: %v", p.ID, err)
							return
						}
						m.logger.Printf("Successfully reconnected to peer %s", p.ID)
					}(peer)
				}
			}
			m.mu.RUnlock()
		}
	}
}

// runHealthCheck periodically checks the health of peer connections
func (m *ReplicationManager) runHealthCheck() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkPeerHealth()
		}
	}
}

// checkPeerHealth checks the health of all peer connections
func (m *ReplicationManager) checkPeerHealth() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	connectedCount := 0
	for _, peer := range m.peers {
		if peer.IsConnected() {
			connectedCount++
		}
	}

	m.logger.Printf("Health check: %d/%d peers connected", connectedCount, len(m.peers))
}

// GetPeer returns a peer by ID
func (m *ReplicationManager) GetPeer(peerID string) (*Peer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peer, exists := m.peers[peerID]
	return peer, exists
}

// GetConnectedPeers returns a list of currently connected peers
func (m *ReplicationManager) GetConnectedPeers() []*Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var connectedPeers []*Peer
	for _, peer := range m.peers {
		if peer.IsConnected() {
			connectedPeers = append(connectedPeers, peer)
		}
	}

	return connectedPeers
}

// GetPeerCount returns the total number of peers and number of connected peers
func (m *ReplicationManager) GetPeerCount() (total, connected int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total = len(m.peers)
	for _, peer := range m.peers {
		if peer.IsConnected() {
			connected++
		}
	}

	return total, connected
}
