package lock_manager

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// LockManager handles all lock-related operations
type LockManager struct {
	mu            sync.Mutex // Protects shared state
	cond          *sync.Cond // Condition variable for lock waiting
	lockHolder    int32      // ID of the client holding the lock, -1 if free
	lockToken     string     // Unique token for current lock
	leaseExpires  time.Time  // When the current lease expires
	logger        *log.Logger
	queue         []int32        // FIFO queue for fairness
	waitingSet    map[int32]bool // Map for tracking clients in queue
	leaseDuration time.Duration  // Duration of lease (e.g., 30 seconds)
}

// NewLockManager initializes a new lock manager
func NewLockManager(logger *log.Logger) *LockManager {
	return NewLockManagerWithLeaseDuration(logger, 30*time.Second)
}

// NewLockManagerWithLeaseDuration initializes a new lock manager with a specified lease duration
func NewLockManagerWithLeaseDuration(logger *log.Logger, leaseDuration time.Duration) *LockManager {
	if logger == nil {
		logger = log.New(os.Stdout, "[LockManager] ", log.LstdFlags)
	}

	lm := &LockManager{
		lockHolder:    -1, // No client holds the lock initially
		lockToken:     "",
		leaseExpires:  time.Time{},
		logger:        logger,
		queue:         make([]int32, 0),
		waitingSet:    make(map[int32]bool),
		leaseDuration: leaseDuration,
	}
	lm.cond = sync.NewCond(&lm.mu)

	// Start the lease monitor goroutine
	go lm.monitorLeases()

	return lm
}

// monitorLeases periodically checks for expired leases and releases locks if needed
func (lm *LockManager) monitorLeases() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		lm.mu.Lock()
		if lm.lockHolder != -1 && !lm.leaseExpires.IsZero() && time.Now().After(lm.leaseExpires) {
			// Lease has expired, release the lock
			expiredHolder := lm.lockHolder
			lm.logger.Printf("Lease expired for client %d, releasing lock", expiredHolder)
			lm.releaseLock()
		}
		lm.mu.Unlock()
	}
}

// releaseLock is an internal helper to release the lock and notify waiting clients
func (lm *LockManager) releaseLock() {
	lm.lockHolder = -1
	lm.lockToken = ""
	lm.leaseExpires = time.Time{}
	lm.logger.Printf("Lock released")
	lm.cond.Broadcast() // Wake all waiting clients
}

// Acquire attempts to acquire the lock for the given client
func (lm *LockManager) Acquire(clientID int32) (bool, string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Client %d attempting to acquire lock", clientID)

	// Check if client already holds the lock (idempotence for retries)
	if lm.lockHolder == clientID {
		lm.logger.Printf("Client %d already holds the lock - handling retry", clientID)
		// Extend the lease on retry
		lm.leaseExpires = time.Now().Add(lm.leaseDuration)
		return true, lm.lockToken
	}

	// Add client to queue if not already in the queue
	if !lm.waitingSet[clientID] {
		lm.queue = append(lm.queue, clientID)
		lm.waitingSet[clientID] = true
	}

	// Check if the current lock has expired
	if lm.lockHolder != -1 && !lm.leaseExpires.IsZero() && time.Now().After(lm.leaseExpires) {
		lm.logger.Printf("Lock held by client %d has expired, releasing", lm.lockHolder)
		lm.releaseLock()
	}

	// Wait until the lock is free AND this client is at the front of the queue
	for lm.lockHolder != -1 || (len(lm.queue) > 0 && lm.queue[0] != clientID) {
		lm.logger.Printf("Client %d waiting for lock (currently held by %d)", clientID, lm.lockHolder)
		lm.cond.Wait() // Unlocks mu, waits, then relocks mu when woken
	}

	// Remove client from queue and waiting set
	lm.queue = lm.queue[1:]
	delete(lm.waitingSet, clientID)

	// Assign the lock to this client with a new token and lease
	lm.lockHolder = clientID
	lm.lockToken = uuid.New().String()
	lm.leaseExpires = time.Now().Add(lm.leaseDuration)
	lm.logger.Printf("Lock acquired by client %d with token %s, expires at %v",
		clientID, lm.lockToken, lm.leaseExpires)

	return true, lm.lockToken
}

// AcquireWithTimeout attempts to acquire the lock with a timeout
func (lm *LockManager) AcquireWithTimeout(clientID int32, ctx context.Context) (bool, string) {
	lm.mu.Lock()

	lm.logger.Printf("Client %d attempting to acquire lock with timeout", clientID)

	// Check if client already holds the lock (idempotence for retries)
	if lm.lockHolder == clientID {
		lm.logger.Printf("Client %d already holds the lock - handling retry", clientID)
		// Extend the lease on retry
		lm.leaseExpires = time.Now().Add(lm.leaseDuration)
		token := lm.lockToken
		lm.mu.Unlock()
		return true, token
	}

	// Check if the current lock has expired
	if lm.lockHolder != -1 && !lm.leaseExpires.IsZero() && time.Now().After(lm.leaseExpires) {
		lm.logger.Printf("Lock held by client %d has expired, releasing", lm.lockHolder)
		lm.releaseLock()
	}

	// Add client to queue if not already waiting
	if !lm.waitingSet[clientID] {
		lm.queue = append(lm.queue, clientID)
		lm.waitingSet[clientID] = true
	}

	for lm.lockHolder != -1 || (len(lm.queue) > 0 && lm.queue[0] != clientID) {
		// Set up a channel to signal when cond.Wait() returns
		waitCh := make(chan struct{})
		go func() {
			lm.mu.Lock()
			lm.cond.Wait() // Unlocks mu, waits, then relocks mu when woken
			lm.mu.Unlock()
			close(waitCh)
		}()

		// Temporarily unlock while waiting
		lm.mu.Unlock()

		// Wait for either the condition to be signaled or timeout
		select {
		case <-waitCh:
			// Reacquire the lock and continue
			lm.mu.Lock()
		case <-ctx.Done():
			// Timeout occurred, reacquire lock to clean up
			lm.mu.Lock()

			// Remove client from queue and waiting set
			for i, id := range lm.queue {
				if id == clientID {
					lm.queue = append(lm.queue[:i], lm.queue[i+1:]...)
					break
				}
			}
			delete(lm.waitingSet, clientID)

			lm.logger.Printf("Client %d timed out waiting for lock", clientID)
			lm.mu.Unlock()
			return false, ""
		}
	}

	// Remove client from queue and waiting set
	lm.queue = lm.queue[1:]
	delete(lm.waitingSet, clientID)

	// Assign the lock to this client with a new token and lease
	lm.lockHolder = clientID
	lm.lockToken = uuid.New().String()
	lm.leaseExpires = time.Now().Add(lm.leaseDuration)
	lm.logger.Printf("Lock acquired by client %d with token %s, expires at %v",
		clientID, lm.lockToken, lm.leaseExpires)

	token := lm.lockToken
	lm.mu.Unlock()
	return true, token
}

// Release attempts to release the lock for the given client
func (lm *LockManager) Release(clientID int32, token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Client %d attempting to release lock with token %s", clientID, token)

	// Check if this client holds the lock with the correct token
	if lm.lockHolder == clientID && lm.lockToken == token {
		lm.releaseLock() // Free the lock
		return true
	}

	// Client doesn't hold the lock or token is invalid
	lm.logger.Printf("Lock release failed: client %d doesn't hold the lock or token is invalid", clientID)
	return false
}

// RenewLease renews the lease for a client if they hold the lock with the correct token
func (lm *LockManager) RenewLease(clientID int32, token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if client holds the lock with the correct token and the lease hasn't expired
	if lm.lockHolder == clientID && lm.lockToken == token && !lm.leaseExpires.IsZero() && time.Now().Before(lm.leaseExpires) {
		// Extend the lease
		lm.leaseExpires = time.Now().Add(lm.leaseDuration)
		lm.logger.Printf("Lease renewed for client %d until %v", clientID, lm.leaseExpires)
		return true
	}

	lm.logger.Printf("Lease renewal failed for client %d: either doesn't hold the lock, token is invalid, or lease expired", clientID)
	return false
}

// HasLock checks if the given client holds the lock with a valid token and lease
func (lm *LockManager) HasLock(clientID int32) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Basic check without token validation (for backward compatibility)
	return lm.lockHolder == clientID
}

// HasLockWithToken checks if the given client holds the lock with a valid token and lease
func (lm *LockManager) HasLockWithToken(clientID int32, token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check that client holds the lock with the correct token and lease hasn't expired
	return lm.lockHolder == clientID &&
		lm.lockToken == token &&
		!lm.leaseExpires.IsZero() &&
		time.Now().Before(lm.leaseExpires)
}

// ReleaseLockIfHeld releases the lock if the given client holds it
func (lm *LockManager) ReleaseLockIfHeld(clientID int32) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// If this client holds the lock, release it
	if lm.lockHolder == clientID {
		lm.releaseLock()
		lm.logger.Printf("Lock released due to client %d closing", clientID)
	}

	// Also remove client from queue if they're waiting
	if lm.waitingSet[clientID] {
		for i, id := range lm.queue {
			if id == clientID {
				lm.queue = append(lm.queue[:i], lm.queue[i+1:]...)
				break
			}
		}
		delete(lm.waitingSet, clientID)
		lm.logger.Printf("Client %d removed from queue due to closing", clientID)
	}
}

// IsLocked returns true if the lock is currently held
func (lm *LockManager) IsLocked() bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.lockHolder != -1
}

// CurrentHolder returns the ID of the client holding the lock, or -1 if free
func (lm *LockManager) CurrentHolder() int32 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.lockHolder
}

// GetLeaseDuration returns the current lease duration setting
func (lm *LockManager) GetLeaseDuration() time.Duration {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.leaseDuration
}

// SetLeaseDuration changes the lease duration for future lock acquisitions
func (lm *LockManager) SetLeaseDuration(duration time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.leaseDuration = duration
	lm.logger.Printf("Lease duration updated to %v", duration)
}

// ForceSetLockHolder directly sets the lock holder to the given client ID.
// This method is intended for TESTING ONLY to help simulate packet loss scenarios.
func (lm *LockManager) ForceSetLockHolder(clientID int32) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.lockHolder = clientID
	lm.lockToken = uuid.New().String()
	lm.leaseExpires = time.Now().Add(lm.leaseDuration)
	lm.logger.Printf("TESTING: Forcibly set lock holder to client %d with token %s", clientID, lm.lockToken)
}
