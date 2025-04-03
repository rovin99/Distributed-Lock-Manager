package lock_manager

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLockManagerPersistence tests the persistence functionality
func TestLockManagerPersistence(t *testing.T) {
	// Setup: Create a temporary test directory for state files
	testDir, err := os.MkdirTemp("", "lockmanager-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp test directory: %v", err)
	}
	defer os.RemoveAll(testDir) // Clean up after tests complete

	// Helper function to create a logger
	createLogger := func() *log.Logger {
		return log.New(os.Stdout, "[Test] ", log.LstdFlags)
	}

	// Test 1: Basic persistence of lock state on acquisition
	t.Run("Basic acquisition persistence", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test1.json")

		// Create lock manager with custom state file
		lm := NewLockManagerWithLeaseDuration(createLogger(), 30*time.Second)
		lm.SetStateFilePath(stateFile)

		// Verify state file exists after initialization
		if _, err := os.Stat(stateFile); os.IsNotExist(err) {
			t.Fatalf("State file was not created: %v", err)
		}

		// Acquire a lock
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Verify state file content directly
		data, err := os.ReadFile(stateFile)
		if err != nil {
			t.Fatalf("Failed to read state file: %v", err)
		}

		var state PersistentLockState
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatalf("Failed to parse state file: %v", err)
		}

		// Verify state matches
		if state.LockHolder != 1 {
			t.Errorf("State file has wrong lock holder: expected 1, got %d", state.LockHolder)
		}
		if state.LockToken != token {
			t.Errorf("State file has wrong token: expected %s, got %s", token, state.LockToken)
		}
		if state.LeaseExpires.Before(time.Now()) {
			t.Error("State file has an expired lease timestamp")
		}

		// Validate using API
		if err := lm.ValidatePersistentState(); err != nil {
			t.Errorf("Persistent state validation failed: %v", err)
		}

		// Clean up - release lock
		lm.Release(1, token)
	})

	// Test 2: State recovery on restart
	t.Run("State recovery on restart", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test2.json")

		// Create first lock manager with custom state file
		lm1 := NewLockManagerWithLeaseDuration(createLogger(), 30*time.Second)
		lm1.SetStateFilePath(stateFile)

		// Acquire a lock with client 1
		success, token := lm1.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Record the lease expiration time for later comparison
		leaseExpires := lm1.GetTokenExpiration()

		// Create a second lock manager simulating a server restart
		// This should load the state from the file
		lm2 := NewLockManagerWithLeaseDuration(createLogger(), 30*time.Second)
		lm2.SetStateFilePath(stateFile)

		// Verify the state was properly recovered
		if lm2.CurrentHolder() != 1 {
			t.Errorf("Recovered lock manager has wrong holder: expected 1, got %d", lm2.CurrentHolder())
		}

		// Client should still hold the lock with the same token
		if !lm2.HasLockWithToken(1, token) {
			t.Error("Client 1 should still hold the lock after recovery")
		}

		// Verify lease expiration time is preserved (approximately)
		newLeaseExpires := lm2.GetTokenExpiration()
		leaseTimeDiff := leaseExpires.Sub(newLeaseExpires)
		if leaseTimeDiff > time.Second || leaseTimeDiff < -time.Second {
			t.Errorf("Lease expiration time not preserved: original=%v, loaded=%v",
				leaseExpires, newLeaseExpires)
		}

		// Client should be able to release lock after server restart
		if !lm2.Release(1, token) {
			t.Error("Client failed to release lock after server restart")
		}

		// Verify lock is actually released in the state file
		if lm2.IsLocked() {
			t.Error("Lock should be released after client release call")
		}
	})

	// Test 3: Expired lease recovery
	t.Run("Expired lease recovery", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test3.json")

		// Create a lock manager with short lease duration
		lm1 := NewLockManagerWithLeaseDuration(createLogger(), 1*time.Second)
		lm1.SetStateFilePath(stateFile)

		// Acquire a lock
		success, _ := lm1.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Wait for the lease to expire
		time.Sleep(2 * time.Second)

		// Create a new lock manager to simulate restart
		// It should detect the expired lease and release the lock
		lm2 := NewLockManagerWithLeaseDuration(createLogger(), 1*time.Second)
		lm2.SetStateFilePath(stateFile)

		// Verify the expired lock was released
		if lm2.IsLocked() {
			t.Error("Lock should be released on recovery due to expired lease")
		}

		// A new client should be able to acquire the lock
		success, token := lm2.Acquire(2)
		if !success {
			t.Fatal("Client 2 failed to acquire lock after recovery")
		}

		// Clean up
		lm2.Release(2, token)
	})

	// Test 4: Persistence on release
	t.Run("Persistence on release", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test4.json")

		// Create lock manager
		lm := NewLockManagerWithLeaseDuration(createLogger(), 30*time.Second)
		lm.SetStateFilePath(stateFile)

		// Acquire and release a lock
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Release the lock
		if !lm.Release(1, token) {
			t.Fatal("Failed to release lock")
		}

		// Verify state file reflects released state
		data, err := os.ReadFile(stateFile)
		if err != nil {
			t.Fatalf("Failed to read state file: %v", err)
		}

		var state PersistentLockState
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatalf("Failed to parse state file: %v", err)
		}

		// Verify released state
		if state.LockHolder != -1 {
			t.Errorf("State file has wrong lock holder after release: expected -1, got %d", state.LockHolder)
		}
		if state.LockToken != "" {
			t.Errorf("State file has non-empty token after release: %s", state.LockToken)
		}
		if !state.LeaseExpires.IsZero() {
			t.Errorf("State file has non-zero lease expiry after release: %v", state.LeaseExpires)
		}
	})

	// Test 5: Persistence on lease renewal
	t.Run("Persistence on lease renewal", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test5.json")

		// Create lock manager
		lm := NewLockManagerWithLeaseDuration(createLogger(), 5*time.Second)
		lm.SetStateFilePath(stateFile)

		// Acquire a lock
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Record initial lease expiry
		initialExpiry := lm.GetTokenExpiration()

		// Wait a bit
		time.Sleep(1 * time.Second)

		// Renew the lease
		if !lm.RenewLease(1, token) {
			t.Fatal("Failed to renew lease")
		}

		// Verify new lease expiry is later than initial
		renewedExpiry := lm.GetTokenExpiration()
		if !renewedExpiry.After(initialExpiry) {
			t.Errorf("Renewed lease (%v) should be later than initial lease (%v)",
				renewedExpiry, initialExpiry)
		}

		// Verify state file reflects updated lease
		data, err := os.ReadFile(stateFile)
		if err != nil {
			t.Fatalf("Failed to read state file: %v", err)
		}

		var state PersistentLockState
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatalf("Failed to parse state file: %v", err)
		}

		// Verify the lease expiry was updated in the state file
		if !state.LeaseExpires.After(initialExpiry) {
			t.Errorf("State file lease expiry (%v) should be later than initial (%v)",
				state.LeaseExpires, initialExpiry)
		}

		// Clean up
		lm.Release(1, token)
	})

	// Test 6: Persistence on lease expiration
	t.Run("Persistence on lease expiration", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test6.json")

		// Create lock manager with very short lease
		lm := NewLockManagerWithLeaseDuration(createLogger(), 1*time.Second)
		lm.SetStateFilePath(stateFile)

		// Acquire a lock
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Verify token is valid initially
		if !lm.IsTokenValid(token) {
			t.Fatal("Token should be valid immediately after acquisition")
		}

		// Wait for lease to expire
		time.Sleep(2 * time.Second)

		// Token should no longer be valid
		if lm.IsTokenValid(token) {
			t.Fatal("Token should be invalid after lease expiration")
		}

		// Verify state file reflects released state
		data, err := os.ReadFile(stateFile)
		if err != nil {
			t.Fatalf("Failed to read state file: %v", err)
		}

		var state PersistentLockState
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatalf("Failed to parse state file: %v", err)
		}

		// Verify released state in file
		if state.LockHolder != -1 {
			t.Errorf("State file has wrong lock holder after expiry: expected -1, got %d", state.LockHolder)
		}
		if state.LockToken != "" {
			t.Errorf("State file has non-empty token after expiry: %s", state.LockToken)
		}
	})

	// Test 7: Concurrent operations with persistence
	t.Run("Concurrent operations with persistence", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test7.json")

		// Create lock manager
		lm := NewLockManagerWithLeaseDuration(createLogger(), 5*time.Second)
		lm.SetStateFilePath(stateFile)

		// Set up 3 clients with different operations
		const numClients = 3
		done := make(chan bool, numClients)

		// Channels for synchronization
		client1Ready := make(chan bool)
		client1HasLock := make(chan bool)

		// Client 1: Acquire, wait, release
		go func() {
			// Signal that client 1 is ready
			client1Ready <- true

			success, token := lm.Acquire(1)
			if !success {
				t.Errorf("Client 1 failed to acquire lock")
				done <- true
				return
			}

			// Signal that client 1 has the lock
			client1HasLock <- true

			time.Sleep(500 * time.Millisecond)

			if !lm.Release(1, token) {
				t.Errorf("Client 1 failed to release lock")
			}
			done <- true
		}()

		// Wait for client 1 to be ready
		<-client1Ready

		// Client 3: Try with timeout then acquire
		go func() {
			// Wait for client 1 to have the lock
			<-client1HasLock

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			// This should time out since client 1 holds the lock
			success, _ := lm.AcquireWithTimeout(3, ctx)
			if success {
				t.Errorf("Client 3 should not have acquired lock with short timeout")
			}

			// Wait for others to finish
			time.Sleep(3 * time.Second)

			// Now should be able to acquire
			success, token := lm.Acquire(3)
			if !success {
				t.Errorf("Client 3 failed to acquire lock after waiting")
				done <- true
				return
			}

			if !lm.Release(3, token) {
				t.Errorf("Client 3 failed to release lock")
			}
			done <- true
		}()

		// Client 2: Wait, acquire, renew, release
		go func() {
			time.Sleep(1 * time.Second) // Wait for client 1 to release

			success, token := lm.Acquire(2)
			if !success {
				t.Errorf("Client 2 failed to acquire lock")
				done <- true
				return
			}

			time.Sleep(500 * time.Millisecond)

			if !lm.RenewLease(2, token) {
				t.Errorf("Client 2 failed to renew lease")
			}

			time.Sleep(500 * time.Millisecond)

			if !lm.Release(2, token) {
				t.Errorf("Client 2 failed to release lock")
			}
			done <- true
		}()

		// Wait for all clients to finish
		for i := 0; i < numClients; i++ {
			<-done
		}

		// Verify final state is clean
		if lm.IsLocked() {
			t.Errorf("Lock should be released after all operations")
		}

		// Verify state file reflects final state
		if err := lm.ValidatePersistentState(); err != nil {
			t.Errorf("Final state validation failed: %v", err)
		}
	})

	// Test 8: File path configuration
	t.Run("File path configuration", func(t *testing.T) {
		// Create lock manager with default path
		lm1 := NewLockManagerWithLeaseDuration(createLogger(), 5*time.Second)

		// State file should exist at default location
		defaultPath := "./data/lock_state.json"
		if _, err := os.Stat(defaultPath); os.IsNotExist(err) {
			t.Errorf("State file not created at default path: %v", err)
		}

		// Clean up default state file
		defer os.Remove(defaultPath)

		// Set custom path
		customPath := filepath.Join(testDir, "custom_path.json")
		lm1.SetStateFilePath(customPath)

		// Acquire to trigger state save
		success, token := lm1.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Verify state file exists at custom location
		if _, err := os.Stat(customPath); os.IsNotExist(err) {
			t.Errorf("State file not created at custom path: %v", err)
		}

		// Clean up
		lm1.Release(1, token)
	})

	// Test 9: Error handling with no write permissions
	t.Run("Error handling with no write permissions", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permission test when running as root")
		}

		// Create a read-only directory
		readOnlyDir := filepath.Join(testDir, "readonly")
		if err := os.Mkdir(readOnlyDir, 0500); err != nil { // 0500 = read+execute only
			t.Fatalf("Failed to create read-only directory: %v", err)
		}
		stateFile := filepath.Join(readOnlyDir, "test9.json")

		// Test acquiring a lock with a path we can't write to
		lm := NewLockManagerWithLeaseDuration(createLogger(), 5*time.Second)
		lm.SetStateFilePath(stateFile)

		// Acquiring should fail since we can't persist state
		success, _ := lm.Acquire(1)
		if success {
			t.Error("Lock acquisition should fail when state cannot be persisted")
		}
	})

	// Test 10: Simulated server crash and restart
	t.Run("Simulated server crash and restart", func(t *testing.T) {
		stateFile := filepath.Join(testDir, "test10.json")

		// Create lock manager
		lm1 := NewLockManagerWithLeaseDuration(createLogger(), 30*time.Second)
		lm1.SetStateFilePath(stateFile)

		// Acquire a lock
		success, token := lm1.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Destroy first lock manager without proper shutdown to simulate crash
		lm1 = nil

		// Create new lock manager with same state file (simulating server restart)
		lm2 := NewLockManagerWithLeaseDuration(createLogger(), 30*time.Second)
		lm2.SetStateFilePath(stateFile)

		// Verify the lock state was recovered
		if lm2.CurrentHolder() != 1 {
			t.Errorf("Lock holder not recovered after restart: expected 1, got %d",
				lm2.CurrentHolder())
		}

		// The token should be preserved
		if !lm2.IsTokenValid(token) {
			t.Error("Token should still be valid after restart")
		}

		// Client should be able to use the lock normally
		if !lm2.RenewLease(1, token) {
			t.Error("Client should be able to renew lease after server restart")
		}

		if !lm2.Release(1, token) {
			t.Error("Client should be able to release lock after server restart")
		}

		// Lock should now be free
		if lm2.IsLocked() {
			t.Error("Lock should be released after client release")
		}
	})
}

// TestInvalidStatePaths tests handling of invalid state file paths
func TestInvalidStatePaths(t *testing.T) {
	// Setup: Create a temporary test directory
	testDir, err := os.MkdirTemp("", "lockmanager-invalid-*")
	if err != nil {
		t.Fatalf("Failed to create temp test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create a custom logger that panics on fatal errors
	logger := log.New(os.Stdout, "[Test] ", log.LstdFlags)

	t.Run("Unparseable state file", func(t *testing.T) {
		// Create an invalid state file
		stateFile := filepath.Join(testDir, "invalid.json")
		if err := os.WriteFile(stateFile, []byte("{this is not valid json}"), 0644); err != nil {
			t.Fatalf("Failed to write invalid state file: %v", err)
		}

		// Create lock manager with invalid state file - should panic during loadState
		defer func() {
			if r := recover(); r == nil {
				t.Error("Lock manager should have panicked on invalid state file")
			}
		}()

		NewLockManagerWithStateFile(logger, 1*time.Second, stateFile)
	})
}
