package lock_manager

import (
	"context"
	"log"
	"os"
	"sync"
)

// LockManager handles all lock-related operations
type LockManager struct {
	mu         sync.Mutex // Protects shared state
	cond       *sync.Cond // Condition variable for lock waiting
	lockHolder int32      // ID of the client holding the lock, -1 if free
	logger     *log.Logger
	queue      []int32 // FIFO queue for fairness
}

// NewLockManager initializes a new lock manager
func NewLockManager(logger *log.Logger) *LockManager {
	if logger == nil {
		logger = log.New(os.Stdout, "[LockManager] ", log.LstdFlags)
	}

	lm := &LockManager{
		lockHolder: -1, // No client holds the lock initially
		logger:     logger,
		queue:      make([]int32, 0),
	}
	lm.cond = sync.NewCond(&lm.mu)
	return lm
}

// Acquire attempts to acquire the lock for the given client
func (lm *LockManager) Acquire(clientID int32) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Client %d attempting to acquire lock", clientID)

	// Add client to queue for fairness
	lm.queue = append(lm.queue, clientID)

	// Wait until the lock is free AND this client is at the front of the queue
	for lm.lockHolder != -1 || (len(lm.queue) > 0 && lm.queue[0] != clientID) {
		lm.logger.Printf("Client %d waiting for lock (currently held by %d)", clientID, lm.lockHolder)
		lm.cond.Wait() // Unlocks mu, waits, then relocks mu when woken
	}

	// Remove client from queue
	lm.queue = lm.queue[1:]

	// Assign the lock to this client
	lm.lockHolder = clientID
	lm.logger.Printf("Lock acquired by client %d", clientID)

	return true
}

// AcquireWithTimeout attempts to acquire the lock with a timeout
func (lm *LockManager) AcquireWithTimeout(clientID int32, ctx context.Context) bool {
	lm.mu.Lock()

	lm.logger.Printf("Client %d attempting to acquire lock with timeout", clientID)

	// Add client to queue for fairness
	lm.queue = append(lm.queue, clientID)

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

			// Remove client from queue
			for i, id := range lm.queue {
				if id == clientID {
					lm.queue = append(lm.queue[:i], lm.queue[i+1:]...)
					break
				}
			}

			lm.logger.Printf("Client %d timed out waiting for lock", clientID)
			lm.mu.Unlock()
			return false
		}
	}

	// Remove client from queue
	lm.queue = lm.queue[1:]

	// Assign the lock to this client
	lm.lockHolder = clientID
	lm.logger.Printf("Lock acquired by client %d", clientID)

	lm.mu.Unlock()
	return true
}

// Release attempts to release the lock for the given client
func (lm *LockManager) Release(clientID int32) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Client %d attempting to release lock", clientID)

	// Check if this client holds the lock
	if lm.lockHolder == clientID {
		lm.lockHolder = -1 // Free the lock
		lm.logger.Printf("Lock released by client %d", clientID)
		lm.cond.Broadcast() // Wake all waiting clients
		return true
	}

	// Client doesn't hold the lock
	lm.logger.Printf("Lock release failed: client %d doesn't hold the lock (current holder: %d)",
		clientID, lm.lockHolder)
	return false
}

// HasLock checks if the given client holds the lock
func (lm *LockManager) HasLock(clientID int32) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.lockHolder == clientID
}

// ReleaseLockIfHeld releases the lock if the given client holds it
func (lm *LockManager) ReleaseLockIfHeld(clientID int32) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// If this client holds the lock, release it
	if lm.lockHolder == clientID {
		lm.lockHolder = -1
		lm.logger.Printf("Lock released due to client %d closing", clientID)
		lm.cond.Broadcast()
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
