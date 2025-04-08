package lock_manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

// PersistentLockState represents the lock state that needs to be persisted
type PersistentLockState struct {
	LockHolder   int32     `json:"lock_holder"`   // -1 if free
	LockToken    string    `json:"lock_token"`    // Empty if free
	LeaseExpires time.Time `json:"lease_expires"` // Zero if free or no lease
	// Note: We are intentionally *not* persisting the queue or waitingSet
	// to keep recovery simpler. Waiting clients will need to retry.
}

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

	// Field for persistence
	stateFilePath string // Path to store the lock state
}

const defaultStateFilePath = "./data/lock_state.json"

// NewLockManager initializes a new lock manager
func NewLockManager(logger *log.Logger) *LockManager {
	return NewLockManagerWithLeaseDuration(logger, 30*time.Second)
}

// NewLockManagerWithLeaseDuration creates a new lock manager with the specified lease duration
func NewLockManagerWithLeaseDuration(logger *log.Logger, leaseDuration time.Duration) *LockManager {
	if logger == nil {
		logger = log.New(os.Stdout, "[LockManager] ", log.LstdFlags)
	}

	lm := &LockManager{
		lockHolder:    -1, // -1 means no one has the lock
		lockToken:     "",
		leaseExpires:  time.Time{},
		leaseDuration: leaseDuration,
		mu:            sync.Mutex{},
		logger:        logger,
		queue:         make([]int32, 0),
		waitingSet:    make(map[int32]bool),
		stateFilePath: defaultStateFilePath,
	}
	lm.cond = sync.NewCond(&lm.mu)

	// Ensure the state file directory exists if using the default path
	dir := filepath.Dir(lm.stateFilePath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		logger.Fatalf("Failed to create directory for lock state file: %v", err)
	}

	// Load the persistent state before starting the lease monitor
	if err := lm.loadState(); err != nil {
		logger.Printf("WARNING: Failed to load or initialize state from '%s': %v", lm.stateFilePath, err)
	}

	// Start the lease monitor
	go lm.monitorLeases()

	return lm
}

// NewLockManagerWithStateFile creates a new lock manager with the specified lease duration and state file path
func NewLockManagerWithStateFile(logger *log.Logger, leaseDuration time.Duration, stateFile string) *LockManager {
	if logger == nil {
		logger = log.New(os.Stdout, "[LockManager] ", log.LstdFlags)
	}

	lm := &LockManager{
		lockHolder:    -1, // -1 means no one has the lock
		lockToken:     "",
		leaseExpires:  time.Time{},
		leaseDuration: leaseDuration,
		mu:            sync.Mutex{},
		logger:        logger,
		queue:         make([]int32, 0),
		waitingSet:    make(map[int32]bool),
		stateFilePath: stateFile,
	}
	lm.cond = sync.NewCond(&lm.mu)

	// Ensure the state file directory exists
	if stateFile != "" {
		dir := filepath.Dir(stateFile)
		if err := os.MkdirAll(dir, 0750); err != nil {
			logger.Fatalf("Failed to create directory for lock state file: %v", err)
		}
	}

	// Load the persistent state before starting the lease monitor
	if err := lm.loadState(); err != nil {
		// Log errors during loading (e.g., permission denied, corrupted file), but don't make it fatal.
		// A nil error means the file didn't exist (ok) or loaded successfully.
		logger.Printf("WARNING: Failed to load or initialize state from '%s': %v", stateFile, err)
		// Depending on requirements, might decide to panic or return error here.
		// For now, we continue with the potentially uninitialized (but valid) in-memory state.
	}

	// Start the lease monitor
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
			// Use the combined method which also saves state
			lm.releaseLockAndSaveState()
		}
		lm.mu.Unlock()
	}
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
		originalLeaseExpires := lm.leaseExpires // For rollback
		lm.leaseExpires = time.Now().Add(lm.leaseDuration)

		// Save state after extending lease
		if err := lm.saveState(); err != nil {
			lm.logger.Printf("ERROR: Failed to save state on retry lease extension for client %d: %v", clientID, err)
			// Rollback the change
			lm.leaseExpires = originalLeaseExpires
			return false, ""
		}

		return true, lm.lockToken
	}

	// Add client to queue if not already in the queue
	if !lm.waitingSet[clientID] {
		lm.queue = append(lm.queue, clientID)
		lm.waitingSet[clientID] = true
		lm.logger.Printf("Added client %d to queue. Queue length: %d", clientID, len(lm.queue))
	}

	// Check if the current lock has expired
	if lm.lockHolder != -1 && !lm.leaseExpires.IsZero() && time.Now().After(lm.leaseExpires) {
		lm.logger.Printf("Lock held by client %d has expired, releasing", lm.lockHolder)
		// Use the combined method which also saves state
		lm.releaseLockAndSaveState()
	}

	// Wait until the lock is free AND this client is at the front of the queue
	for lm.lockHolder != -1 || (len(lm.queue) > 0 && lm.queue[0] != clientID) {
		lm.logger.Printf("Client %d waiting for lock (currently held by %d, queue position: %d)",
			clientID, lm.lockHolder, findClientPosition(lm.queue, clientID))
		lm.cond.Wait() // Unlocks mu, waits, then relocks mu when woken

		// After waking up, check if the lock has expired
		if lm.lockHolder != -1 && !lm.leaseExpires.IsZero() && time.Now().After(lm.leaseExpires) {
			lm.logger.Printf("Lock held by client %d has expired while client %d was waiting, releasing",
				lm.lockHolder, clientID)
			lm.releaseLockAndSaveState()
		}
	}

	// Safety check - verify the client is still at the head of the queue
	if len(lm.queue) == 0 || lm.queue[0] != clientID {
		lm.logger.Printf("ERROR: Client %d was expecting to be at the front of the queue but isn't", clientID)
		return false, ""
	}

	// Remove client from queue and waiting set
	lm.queue = lm.queue[1:]
	delete(lm.waitingSet, clientID)

	// Prepare new lock state but save before committing
	newToken := uuid.New().String()
	newLeaseExpires := time.Now().Add(lm.leaseDuration)

	// Update memory state so we can save it
	lm.lockHolder = clientID
	lm.lockToken = newToken
	lm.leaseExpires = newLeaseExpires

	// Save the state
	if err := lm.saveState(); err != nil {
		// Rollback changes if save fails
		lm.logger.Printf("ERROR: Failed to save lock state for client %d: %v", clientID, err)
		lm.lockHolder = -1
		lm.lockToken = ""
		lm.leaseExpires = time.Time{}
		return false, ""
	}

	lm.logger.Printf("Lock acquired by client %d with token %s, expires at %v",
		clientID, lm.safeTokenForLog(newToken), newLeaseExpires)

	return true, newToken
}

// Helper function to find a client's position in the queue
func findClientPosition(queue []int32, clientID int32) int {
	for i, id := range queue {
		if id == clientID {
			return i
		}
	}
	return -1 // Not in queue
}

// AcquireWithTimeout attempts to acquire the lock with a timeout
func (lm *LockManager) AcquireWithTimeout(clientID int32, ctx context.Context) (bool, string) {
	lm.mu.Lock()

	lm.logger.Printf("Client %d attempting to acquire lock with timeout", clientID)

	// Check if client already holds the lock (idempotence for retries)
	if lm.lockHolder == clientID {
		lm.logger.Printf("Client %d already holds the lock - handling retry", clientID)
		// Extend the lease on retry
		originalLeaseExpires := lm.leaseExpires // For rollback
		lm.leaseExpires = time.Now().Add(lm.leaseDuration)

		// Save state after extending lease
		if err := lm.saveState(); err != nil {
			lm.logger.Printf("ERROR: Failed to save state on retry lease extension for client %d: %v", clientID, err)
			// Rollback the change
			lm.leaseExpires = originalLeaseExpires
			token := ""
			lm.mu.Unlock()
			return false, token
		}

		token := lm.lockToken
		lm.mu.Unlock()
		return true, token
	}

	// Check if the current lock has expired
	if lm.lockHolder != -1 && !lm.leaseExpires.IsZero() && time.Now().After(lm.leaseExpires) {
		lm.logger.Printf("Lock held by client %d has expired, releasing", lm.lockHolder)
		// Use the combined method which also saves state
		lm.releaseLockAndSaveState()
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

			// If this client was at the front of the queue, wake up the next client
			if len(lm.queue) > 0 {
				lm.cond.Broadcast()
			}

			lm.logger.Printf("Client %d timed out waiting for lock", clientID)
			lm.mu.Unlock()
			return false, ""
		}
	}

	// Remove client from queue and waiting set
	lm.queue = lm.queue[1:]
	delete(lm.waitingSet, clientID)

	// Prepare new lock state but save before committing
	newToken := uuid.New().String()
	newLeaseExpires := time.Now().Add(lm.leaseDuration)

	// Update memory state so we can save it
	lm.lockHolder = clientID
	lm.lockToken = newToken
	lm.leaseExpires = newLeaseExpires

	// Persist the state change
	if err := lm.saveState(); err != nil {
		lm.logger.Printf("ERROR: Failed to save state on acquire with timeout for client %d: %v", clientID, err)
		// Rollback the state change
		lm.lockHolder = -1
		lm.lockToken = ""
		lm.leaseExpires = time.Time{}
		// Wake others as acquire failed
		lm.cond.Broadcast()
		lm.mu.Unlock()
		return false, ""
	}

	lm.logger.Printf("Lock acquired by client %d with token %s, expires at %v",
		clientID, lm.lockToken, lm.leaseExpires)

	token := lm.lockToken
	lm.mu.Unlock()
	return true, token
}

// Release attempts to release the lock held by the client
func (lm *LockManager) Release(clientID int32, token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Client %d attempting to release lock with token %s", clientID, lm.safeTokenForLog(token))

	// Check if client holds the lock with the correct token
	if lm.lockHolder != clientID {
		lm.logger.Printf("ERROR: Client %d tried to release lock, but current holder is %d", clientID, lm.lockHolder)
		return false
	}

	if lm.lockToken != token {
		lm.logger.Printf("ERROR: Client %d provided invalid token for release", clientID)
		return false
	}

	// Clear lock state
	lm.lockHolder = -1
	lm.lockToken = ""
	lm.leaseExpires = time.Time{}

	// Save the state
	if err := lm.saveState(); err != nil {
		// This is a critical error - we've released the lock in memory but failed to persist it
		// Log the error and continue, as rolling back would mean reclaiming a lock that the client thinks is released
		lm.logger.Printf("CRITICAL ERROR: Failed to save state on release for client %d: %v", clientID, err)
	}

	lm.logger.Printf("Lock released by client %d", clientID)

	// Notify waiting clients
	lm.cond.Broadcast()
	return true
}

// RenewLease renews the lease for a client if they hold the lock with the correct token
func (lm *LockManager) RenewLease(clientID int32, token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if this client owns the lock
	if lm.lockHolder != clientID {
		lm.logger.Printf("Lease renewal failed: client %d doesn't hold the lock", clientID)
		return false
	}

	// Check if the token is valid and lease hasn't expired
	if lm.lockToken != token || lm.leaseExpires.IsZero() || time.Now().After(lm.leaseExpires) {
		lm.logger.Printf("Lease renewal failed: token %s is invalid or expired", token)
		return false
	}

	// Token is valid and client owns the lock, store original lease for rollback
	originalLeaseExpires := lm.leaseExpires

	// Extend the lease
	lm.leaseExpires = time.Now().Add(lm.leaseDuration)

	// Persist the state change
	if err := lm.saveState(); err != nil {
		lm.logger.Printf("ERROR: Failed to save state on lease renewal for client %d: %v", clientID, err)
		// Rollback the lease expiration change
		lm.leaseExpires = originalLeaseExpires
		return false
	}

	lm.logger.Printf("Lease renewed for client %d until %v", clientID, lm.leaseExpires)
	return true
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

// IsTokenValid checks if a token is valid for any client
func (lm *LockManager) IsTokenValid(token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if token matches current lock token and lease hasn't expired
	return lm.lockToken == token &&
		!lm.leaseExpires.IsZero() &&
		time.Now().Before(lm.leaseExpires)
}

// GetTokenExpiration returns the expiration time of the current token
func (lm *LockManager) GetTokenExpiration() time.Time {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.leaseExpires
}

// ReleaseLockIfHeld releases the lock if the given client holds it
func (lm *LockManager) ReleaseLockIfHeld(clientID int32) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// If this client holds the lock, release it
	if lm.lockHolder == clientID {
		// Use the combined method which also saves state
		lm.releaseLockAndSaveState()
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

	// Save the forced state change (best effort, but log failures)
	if err := lm.saveState(); err != nil {
		lm.logger.Printf("WARNING: Failed to save state after force-setting lock holder to %d: %v", clientID, err)
	}

	lm.logger.Printf("TESTING: Forcibly set lock holder to client %d with token %s", clientID, lm.lockToken)
}

// saveState persists the current lock state to disk atomically.
// IMPORTANT: This MUST be called while holding lm.mu lock.
func (lm *LockManager) saveState() error {
	state := PersistentLockState{
		LockHolder:   lm.lockHolder,
		LockToken:    lm.lockToken,
		LeaseExpires: lm.leaseExpires,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		lm.logger.Printf("ERROR: Failed to marshal lock state: %v", err)
		return fmt.Errorf("failed to marshal lock state: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(lm.stateFilePath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		lm.logger.Printf("ERROR: Failed to create directory for lock state file '%s': %v", dir, err)
		return fmt.Errorf("failed to create state directory: %w", err)
	}

	// Write to a temporary file first
	tempFilePath := lm.stateFilePath + ".tmp"
	file, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		lm.logger.Printf("ERROR: Failed to open temporary state file '%s': %v", tempFilePath, err)
		return fmt.Errorf("failed to open temp state file: %w", err)
	}

	_, writeErr := file.Write(data)
	// Sync error is critical for durability
	syncErr := file.Sync()
	// Close error is less critical but good practice
	closeErr := file.Close()

	if writeErr != nil {
		lm.logger.Printf("ERROR: Failed to write lock state to temporary file '%s': %v", tempFilePath, writeErr)
		os.Remove(tempFilePath) // Clean up temp file on error
		return fmt.Errorf("failed to write state data: %w", writeErr)
	}
	if syncErr != nil {
		lm.logger.Printf("ERROR: Failed to sync lock state temporary file '%s': %v", tempFilePath, syncErr)
		os.Remove(tempFilePath) // Clean up temp file on error
		return fmt.Errorf("failed to sync state file: %w", syncErr)
	}
	if closeErr != nil {
		lm.logger.Printf("WARNING: Failed to close lock state temporary file '%s': %v", tempFilePath, closeErr)
		// Continue, as data is synced
	}

	// Atomically replace the old state file with the new one
	if err := os.Rename(tempFilePath, lm.stateFilePath); err != nil {
		lm.logger.Printf("ERROR: Failed to rename temporary state file to '%s': %v", lm.stateFilePath, err)
		os.Remove(tempFilePath) // Clean up temp file
		return fmt.Errorf("failed to commit state file: %w", err)
	}

	lm.logger.Printf("DEBUG: Successfully saved lock state (Holder: %d, Token: %s..., Expires: %v)",
		state.LockHolder, lm.safeTokenForLog(state.LockToken), state.LeaseExpires)
	return nil
}

// safeTokenForLog returns a safe version of token for logging
// (shortened to avoid cluttering logs with UUID strings)
func (lm *LockManager) safeTokenForLog(token string) string {
	if len(token) <= 8 {
		return token
	}
	return token[:8] + "..."
}

// loadState loads the lock state from a persistent file. If the file doesn't exist,
// it assumes an initial state and saves it.
func (lm *LockManager) loadState() error {
	// No mutex here; called from functions that already hold the lock

	// If no state file path is set, just use in-memory state
	if lm.stateFilePath == "" {
		lm.logger.Printf("No state file path set, using in-memory state only")
		return nil
	}

	// Try to read the lock state file
	data, err := os.ReadFile(lm.stateFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// If the file doesn't exist, log and assume initial state in memory.
			// The file will be created on the first saveState call.
			lm.logger.Printf("Lock state file '%s' not found, assuming initial state.", lm.stateFilePath)
			lm.lockHolder = -1
			lm.lockToken = ""
			lm.leaseExpires = time.Time{}
			return nil // Not an error, just means we start fresh
		}
		return fmt.Errorf("failed to read lock state file: %w", err)
	}

	// Parse the JSON data
	var state PersistentLockState
	if err := json.Unmarshal(data, &state); err != nil {
		// Critical error - panic instead of returning an error to maintain data integrity
		panic(fmt.Sprintf("Critical error: failed to parse lock state file: %v", err))
	}

	// Check if the state from file is valid
	now := time.Now()
	if state.LockHolder != -1 && state.LeaseExpires.Before(now) {
		// Lock has expired while the server was down
		lm.logger.Printf("Loaded state indicates lease for client %d expired while server was down. Releasing lock.", state.LockHolder)
		lm.lockHolder = -1
		lm.lockToken = ""
		lm.leaseExpires = time.Time{}

		// Save the updated state
		if err := lm.saveState(); err != nil {
			lm.logger.Printf("ERROR: Failed to save state after releasing expired lock: %v", err)
			return err
		}
		lm.logger.Printf("Lock released internally (e.g., expiry on load, monitor)")
		lm.cond.Broadcast()
	} else {
		// Load the state from the file (could be unexpired or no lock holder)
		lm.logger.Printf("Successfully loaded lock state (Holder: %d, Token: %s..., Expires: %v)",
			state.LockHolder, lm.safeTokenForLog(state.LockToken), state.LeaseExpires)
		lm.lockHolder = state.LockHolder     // Assign from loaded state
		lm.lockToken = state.LockToken       // Assign from loaded state
		lm.leaseExpires = state.LeaseExpires // Assign from loaded state
	}

	return nil
}

// releaseLockAndSaveState releases the lock due to lease expiration or other reasons
// and persists the state change. Caller must hold the mutex.
func (lm *LockManager) releaseLockAndSaveState() {
	// Remember the previous holder for logging
	previousHolder := lm.lockHolder

	// Clear the lock state
	lm.lockHolder = -1
	lm.lockToken = ""
	lm.leaseExpires = time.Time{}

	// Persist the state change
	if err := lm.saveState(); err != nil {
		lm.logger.Printf("ERROR: Failed to save state on internal release for client %d: %v", previousHolder, err)
	}

	lm.logger.Printf("Lock forcibly released from client %d due to lease expiration or server request", previousHolder)

	// Notify waiting goroutines
	lm.cond.Broadcast()
}

// SetStateFilePath sets the path where the lock state will be persisted
func (lm *LockManager) SetStateFilePath(path string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Lock state file path set to: %s", path)
	lm.stateFilePath = path

	// Ensure the directory exists
	if path != "" { // Only create dir if path is not empty
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0750); err != nil {
			lm.logger.Printf("ERROR: Failed to create directory for lock state file '%s': %v", dir, err)
			// Decide if this should be a fatal error or just a warning
			return // Return early if we can't create the directory
		}
	}

	// Attempt to load state from the new path. If it doesn't exist,
	// loadState will initialize in-memory state correctly.
	if err := lm.loadState(); err != nil {
		lm.logger.Printf("WARNING: Failed to load state from new path '%s': %v", path, err)
		// Continue with potentially uninitialized (but valid) in-memory state.
	}

	// Save the state file to ensure it exists immediately after setting the path
	if err := lm.saveState(); err != nil {
		lm.logger.Printf("WARNING: Failed to create initial state file at '%s': %v", path, err)
	}
}

// ValidatePersistentState checks if the persistent state matches the in-memory state.
// This is useful for testing and debugging.
func (lm *LockManager) ValidatePersistentState() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Read the current state file
	data, err := os.ReadFile(lm.stateFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("state file does not exist: %s", lm.stateFilePath)
		}
		return fmt.Errorf("failed to read state file: %w", err)
	}

	var fileState PersistentLockState
	if err := json.Unmarshal(data, &fileState); err != nil {
		return fmt.Errorf("failed to parse state file: %w", err)
	}

	// Compare with in-memory state
	if fileState.LockHolder != lm.lockHolder {
		return fmt.Errorf("lock holder mismatch: file=%d, memory=%d", fileState.LockHolder, lm.lockHolder)
	}
	if fileState.LockToken != lm.lockToken {
		return fmt.Errorf("lock token mismatch: file=%s, memory=%s", fileState.LockToken, lm.lockToken)
	}
	// For time, we need a small tolerance for equality check
	if !fileState.LeaseExpires.Equal(lm.leaseExpires) {
		return fmt.Errorf("lease expiry mismatch: file=%v, memory=%v", fileState.LeaseExpires, lm.leaseExpires)
	}

	return nil
}

// ApplyReplicatedState applies a replicated state from the primary server
// It overwrites the current lock state with the provided values and saves to disk
func (lm *LockManager) ApplyReplicatedState(holder int32, token string, expiryTimestamp int64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Applying replicated state: holder=%d, token=%s, expiry=%v",
		holder, lm.safeTokenForLog(token), time.Unix(expiryTimestamp, 0))

	// Store original values for rollback if needed
	originalHolder := lm.lockHolder
	originalToken := lm.lockToken
	originalExpiry := lm.leaseExpires

	// Set the new state
	lm.lockHolder = holder
	lm.lockToken = token

	// Convert Unix timestamp to time.Time
	if expiryTimestamp > 0 {
		lm.leaseExpires = time.Unix(expiryTimestamp, 0)
	} else {
		lm.leaseExpires = time.Time{} // Zero time for unlocked state
	}

	// Save to disk
	if err := lm.saveState(); err != nil {
		// Rollback on error
		lm.lockHolder = originalHolder
		lm.lockToken = originalToken
		lm.leaseExpires = originalExpiry
		lm.logger.Printf("Failed to save replicated state: %v", err)
		return fmt.Errorf("failed to save replicated state: %w", err)
	}

	// Signal waiting goroutines in case the replication changed lock state
	lm.cond.Broadcast()

	return nil
}

// ForceClearLockState forces the lock state to be cleared and persisted as unlocked
// Used after fencing period to ensure any potentially stale lock state is cleared
func (lm *LockManager) ForceClearLockState() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Printf("Forcing clear of lock state")

	// Save original values for logging
	originalHolder := lm.lockHolder
	originalToken := lm.lockToken

	// Clear state
	lm.lockHolder = -1
	lm.lockToken = ""
	lm.leaseExpires = time.Time{}

	// Save the cleared state
	if err := lm.saveState(); err != nil {
		lm.logger.Printf("Failed to save cleared lock state: %v", err)
		return fmt.Errorf("failed to save cleared lock state: %w", err)
	}

	lm.logger.Printf("Successfully cleared lock state (was held by %d with token %s)",
		originalHolder, lm.safeTokenForLog(originalToken))

	// Signal waiting goroutines
	lm.cond.Broadcast()

	return nil
}

// GetMutex returns the mutex for external synchronization
func (lm *LockManager) GetMutex() *sync.Mutex {
	return &lm.mu
}

// CurrentHolderNoLock returns the current lock holder without locking
// The caller must hold the mutex
func (lm *LockManager) CurrentHolderNoLock() int32 {
	return lm.lockHolder
}

// GetCurrentTokenNoLock returns the current lock token without locking
// The caller must hold the mutex
func (lm *LockManager) GetCurrentTokenNoLock() string {
	return lm.lockToken
}

// GetTokenExpirationNoLock returns the lock expiration time without locking
// The caller must hold the mutex
func (lm *LockManager) GetTokenExpirationNoLock() time.Time {
	return lm.leaseExpires
}

// GetQueueLength returns the current length of the waiting queue
func (lm *LockManager) GetQueueLength() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return len(lm.queue)
}
