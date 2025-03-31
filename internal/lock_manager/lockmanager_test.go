package lock_manager

import (
	"context"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	// Create logs directory if it doesn't exist
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Printf("Failed to create logs directory: %v", err)
	}

	// Redirect test logs to file
	logFile, err := os.OpenFile(filepath.Join("logs", "lockmanager_test.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Failed to open test log file: %v", err)
	} else {
		log.SetOutput(logFile)
	}
}

func TestLockManagerBasic(t *testing.T) {
	lm := NewLockManager(nil)

	// Test initial state
	if lm.HasLock(1) {
		t.Error("New client should not have lock initially")
	}

	// Test basic acquire
	success, token := lm.Acquire(1)
	if !success {
		t.Error("Failed to acquire lock")
	}

	// Check token is not empty
	if token == "" {
		t.Error("Acquire should return a non-empty token")
	}

	// Test HasLock
	if !lm.HasLock(1) {
		t.Error("Client should have lock after acquiring it")
	}
	if lm.HasLock(2) {
		t.Error("Client 2 should not have lock")
	}

	// Test HasLockWithToken
	if !lm.HasLockWithToken(1, token) {
		t.Error("Client should have lock with valid token")
	}
	if lm.HasLockWithToken(1, "invalid-token") {
		t.Error("Client should not have lock with invalid token")
	}

	// Test IsLocked and CurrentHolder
	if !lm.IsLocked() {
		t.Error("Lock should be marked as locked")
	}
	if lm.CurrentHolder() != 1 {
		t.Errorf("Current holder should be 1, got %d", lm.CurrentHolder())
	}

	// Test release
	if !lm.Release(1, token) {
		t.Error("Failed to release lock")
	}
	if lm.HasLock(1) {
		t.Error("Client should not have lock after releasing it")
	}
	if lm.IsLocked() {
		t.Error("Lock should be marked as unlocked after release")
	}

	// Test releasing a lock not held
	if lm.Release(1, token) {
		t.Error("Should not be able to release a lock not held")
	}
}

func TestLockManagerConcurrent(t *testing.T) {
	lm := NewLockManager(nil)
	numWorkers := 10
	testDuration := 500 * time.Millisecond

	// Map to track which client is currently holding the lock
	var lockHolder int32 = -1
	var lockHolderMu sync.Mutex
	var currentToken string

	// Create workers
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	lockOperations := int64(0)
	success := int64(0)

	// Start time
	start := time.Now()

	// Launch workers
	for i := 0; i < numWorkers; i++ {
		id := int32(i + 1)
		go func(id int32) {
			defer wg.Done()

			for time.Since(start) < testDuration {
				// Try to acquire the lock
				acquired, token := lm.Acquire(id)
				if !acquired {
					continue
				}

				// Record lock holder
				lockHolderMu.Lock()
				if lockHolder != -1 {
					t.Errorf("Lock already held by %d when %d acquired it", lockHolder, id)
				}
				lockHolder = id
				currentToken = token
				lockHolderMu.Unlock()

				// Simulate some work
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

				// Verify lock holder is still us
				lockHolderMu.Lock()
				if lockHolder != id {
					t.Errorf("Lock holder changed from %d to %d during critical section", id, lockHolder)
				}

				// Update metrics
				atomic.AddInt64(&lockOperations, 1)
				atomic.AddInt64(&success, 1)

				// Release lock
				lockHolder = -1
				tokenToRelease := currentToken
				lockHolderMu.Unlock()
				lm.Release(id, tokenToRelease)

				// Small delay before trying again
				time.Sleep(5 * time.Millisecond)
			}
		}(id)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Log results
	t.Logf("Completed %d lock operations with %d successful acquisitions in %v", lockOperations, success, testDuration)
}

func TestLockContention(t *testing.T) {
	lm := NewLockManager(nil)

	// Create a channel for client 2 to signal it's ready
	client2Ready := make(chan bool, 1)
	client2Done := make(chan bool, 1)

	// Create a channel for events
	events := make(chan string, 3)

	// Have client 1 acquire the lock
	success, token1 := lm.Acquire(1)
	if !success {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Start client 2 in a goroutine to wait for the lock
	go func() {
		// Signal that client 2 is ready
		client2Ready <- true

		// Wait for lock acquisition
		events <- "Client2-Waiting"
		success, token2 := lm.Acquire(2)
		if !success {
			t.Error("Client 2 failed to acquire lock")
		}
		events <- "Client2-Acquired"

		// Hold the lock briefly
		time.Sleep(10 * time.Millisecond)

		// Release the lock
		lm.Release(2, token2)
		events <- "Client2-Released"

		client2Done <- true
	}()

	// Wait for client 2 to be ready
	<-client2Ready

	// Give client 2 time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Now have client 1 release the lock
	lm.Release(1, token1)

	// Verify client 2 now gets the lock
	select {
	case event := <-events:
		if event != "Client2-Waiting" {
			t.Errorf("Expected Client2-Waiting, got %s", event)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for client 2 to start waiting")
	}

	select {
	case event := <-events:
		if event != "Client2-Acquired" {
			t.Errorf("Expected Client2-Acquired, got %s", event)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for client 2 to acquire lock")
	}

	// Clean up
	<-client2Done
}

func TestAcquireWithTimeout(t *testing.T) {
	lm := NewLockManager(nil)

	// First client acquires the lock
	success, token1 := lm.Acquire(1)
	if !success {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Second client tries with a 50ms timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// This should time out
	start := time.Now()
	success, _ = lm.AcquireWithTimeout(2, ctx)
	elapsed := time.Since(start)

	if success {
		t.Error("Client 2 should not have acquired the lock due to timeout")
	}

	// Verify timeout was respected
	if elapsed < 50*time.Millisecond {
		t.Errorf("Timeout occurred too quickly: %v", elapsed)
	}

	// Release the lock
	lm.Release(1, token1)

	// Try again with a longer timeout - should succeed quickly
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start = time.Now()
	success, token2 := lm.AcquireWithTimeout(2, ctx)
	elapsed = time.Since(start)

	if !success {
		t.Error("Client 2 should have acquired the lock after client 1 released it")
	}

	// This time it should be quick since the lock is free
	if elapsed > 100*time.Millisecond {
		t.Errorf("Lock acquisition took too long after release: %v", elapsed)
	}

	// Clean up
	lm.Release(2, token2)
}

func TestFIFOFairness(t *testing.T) {
	lm := NewLockManager(nil)

	// Events channel to track the order of acquisitions
	events := make(chan int, 3)

	// First have client 1 acquire the lock
	success, token1 := lm.Acquire(1)
	if !success {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Then start clients 2 and 3 to wait for the lock
	// Client 2 starts first, so should get the lock before client 3
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, token2 := lm.Acquire(2)
		events <- 2
		time.Sleep(10 * time.Millisecond)
		lm.Release(2, token2)
	}()

	// Give client 2 a head start
	time.Sleep(10 * time.Millisecond)

	go func() {
		defer wg.Done()
		_, token3 := lm.Acquire(3)
		events <- 3
		time.Sleep(10 * time.Millisecond)
		lm.Release(3, token3)
	}()

	// Give both clients time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release the lock
	lm.Release(1, token1)

	// Verify client 2 gets the lock before client 3 (FIFO order)
	select {
	case client := <-events:
		if client != 2 {
			t.Errorf("Expected client 2 to get the lock first, but got client %d", client)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for client 2 to acquire lock")
	}

	// Now release client 2's lock
	// (Already done in the goroutine)

	// Verify client 3 gets the lock
	select {
	case client := <-events:
		if client != 3 {
			t.Errorf("Expected client 3 to get the lock next, but got client %d", client)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for client 3 to acquire lock")
	}

	// Clean up
	// (Already done in the goroutines)
	wg.Wait()
}

func TestStressTest(t *testing.T) {
	lm := NewLockManager(nil)
	runDuration := 2 * time.Second
	numWorkers := 10

	// Track the current lock holder for validation
	var currentHolder int32 = -1
	var holderMu sync.Mutex
	var currentToken string

	// Track statistics
	var totalOps int64 = 0
	var successfulAcqs int64 = 0
	var failedAcqs int64 = 0
	var errors int64 = 0

	// Start workers
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	startTime := time.Now()

	for i := 0; i < numWorkers; i++ {
		id := int32(i + 1)
		go func(id int32) {
			defer wg.Done()

			for time.Since(startTime) < runDuration {
				// Try to acquire lock
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				acquireSuccess, acquiredToken := lm.AcquireWithTimeout(id, ctx)
				cancel()

				if acquireSuccess {
					atomic.AddInt64(&successfulAcqs, 1)

					// Verify lock integrity
					holderMu.Lock()
					if currentHolder != -1 {
						t.Errorf("Lock already held by %d when %d acquired it", currentHolder, id)
						atomic.AddInt64(&errors, 1)
					}
					currentHolder = id
					currentToken = acquiredToken
					holderMu.Unlock()

					// Hold the lock briefly
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

					// Verify nobody else has taken the lock
					holderMu.Lock()
					if currentHolder != id {
						t.Errorf("Lock holder changed from %d to %d while locked", id, currentHolder)
						atomic.AddInt64(&errors, 1)
					}

					// Release the lock
					currentHolder = -1
					tokenToRelease := currentToken
					holderMu.Unlock()
					lm.Release(id, tokenToRelease)

					atomic.AddInt64(&totalOps, 1)
				} else {
					atomic.AddInt64(&failedAcqs, 1)
				}

				// Small delay between attempts
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}(id)
	}

	// Wait for test to complete
	wg.Wait()
	elapsed := time.Since(startTime)

	// Report results
	t.Logf("Stress test completed in %v", elapsed)
	t.Logf("Total successful ops: %d", totalOps)
	t.Logf("Successful acquisitions: %d", successfulAcqs)
	t.Logf("Failed acquisitions (timeouts): %d", failedAcqs)
	t.Logf("Detected errors: %d", errors)

	// The test passes if there were no errors
	if errors > 0 {
		t.Errorf("Detected %d lock integrity errors", errors)
	}
}

func BenchmarkLockAcquireRelease(b *testing.B) {
	lm := NewLockManager(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientID := int32(i % 1000)
		success, token := lm.Acquire(clientID)
		if !success {
			b.Fatalf("Failed to acquire lock for client %d", clientID)
		}
		lm.Release(clientID, token)
	}
}

func BenchmarkConcurrentLockOperations(b *testing.B) {
	lm := NewLockManager(nil)
	numGoroutines := 4

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	opsPerGoroutine := b.N / numGoroutines

	for g := 0; g < numGoroutines; g++ {
		go func(g int) {
			defer wg.Done()

			for i := 0; i < opsPerGoroutine; i++ {
				clientID := int32((g * 1000) + (i % 1000))
				success, token := lm.Acquire(clientID)
				if !success {
					b.Fatalf("Failed to acquire lock for client %d", clientID)
				}
				lm.Release(clientID, token)
			}
		}(g)
	}

	wg.Wait()
}

func TestLongRunningLockHolder(t *testing.T) {
	lm := NewLockManager(nil)

	// Create channel to track when client 2 acquires the lock
	client2Acquired := make(chan bool)

	// Client 1 acquires the lock
	success, token1 := lm.Acquire(1)
	if !success {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Launch a goroutine for Client 2 to try acquiring the lock
	go func() {
		// Wait for Client 1 to release the lock
		success, token2 := lm.Acquire(2)
		if !success {
			t.Error("Client 2 failed to acquire the lock")
		} else {
			client2Acquired <- true
			time.Sleep(50 * time.Millisecond)
			lm.Release(2, token2)
		}
	}()

	// Allow Client 1 to hold the lock for a while
	time.Sleep(200 * time.Millisecond)

	// Release lock from Client 1
	lm.Release(1, token1)

	// Verify Client 2 acquires the lock after release
	select {
	case <-client2Acquired:
		// Success - Client 2 got the lock
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 2 did not acquire the lock in time")
	}
}

func TestClientDisconnection(t *testing.T) {
	lm := NewLockManager(nil)

	// Client 1 acquires the lock
	_, _ = lm.Acquire(1)

	// Simulate client disconnection by forcing a release
	lm.ReleaseLockIfHeld(1)

	// Verify the lock is released
	if lm.IsLocked() {
		t.Error("Lock should be released after client disconnection")
	}

	// Client 2 should be able to acquire the lock immediately
	success, token2 := lm.Acquire(2)
	if !success {
		t.Error("Client 2 failed to acquire lock after client 1 disconnection")
	}

	// Clean up
	lm.Release(2, token2)
}

func TestMultipleTimeoutClients(t *testing.T) {
	lm := NewLockManager(nil)

	// Client 1 acquires the lock
	success, token1 := lm.Acquire(1)
	if !success {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Client 2 attempts to acquire the lock with a 5-second timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	client2Acquired := make(chan bool)
	go func() {
		success, token := lm.AcquireWithTimeout(2, ctx2)
		client2Acquired <- success
		if success {
			time.Sleep(10 * time.Millisecond)
			lm.Release(2, token)
		}
	}()

	// Client 3 attempts to acquire the lock with a 10-second timeout
	ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel3()

	client3Acquired := make(chan bool)
	go func() {
		success, token := lm.AcquireWithTimeout(3, ctx3)
		client3Acquired <- success
		if success {
			lm.Release(3, token)
		}
	}()

	// Wait for 6 seconds and verify Client 2 times out
	time.Sleep(6 * time.Second)
	select {
	case success := <-client2Acquired:
		if success {
			t.Error("Client 2 should have timed out")
		}
	default:
		t.Error("Client 2 didn't respond in time")
	}

	// Release lock from Client 1
	lm.Release(1, token1)

	// Verify Client 3 acquires the lock
	select {
	case success := <-client3Acquired:
		if !success {
			t.Error("Client 3 should have acquired the lock")
		}
	case <-time.After(1 * time.Second):
		t.Error("Client 3 didn't acquire the lock in time")
	}
}

func TestMixedOperation(t *testing.T) {
	lm := NewLockManager(nil)

	// Start a goroutine that continuously acquires and releases the lock
	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				success, token := lm.Acquire(1)
				if success {
					time.Sleep(10 * time.Millisecond)
					lm.Release(1, token)
				}
			}
		}
	}()

	// Start another goroutine that tries acquiring with timeouts
	resultCh := make(chan int)
	go func() {
		successCount := 0
		failCount := 0

		for i := 0; i < 10; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			success, token := lm.AcquireWithTimeout(2, ctx)
			cancel()

			if success {
				successCount++
				lm.Release(2, token)
			} else {
				failCount++
			}

			time.Sleep(20 * time.Millisecond)
		}

		resultCh <- successCount
		resultCh <- failCount
	}()

	// Start a third goroutine with short timeouts
	go func() {
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if success, token := lm.AcquireWithTimeout(4, ctx); success {
				lm.Release(4, token)
			}
		}
	}()

	// Wait for results
	successCount := <-resultCh
	failCount := <-resultCh

	// Stop the background goroutine
	close(stopCh)

	// We expect some successes and some failures
	t.Logf("Client 2 had %d successes and %d failures", successCount, failCount)

	// There should be some successes (but we can't guarantee exactly how many)
	if successCount == 0 {
		t.Error("Expected at least some successful lock acquisitions")
	}
}

func TestPacketLossRetryMechanism(t *testing.T) {
	lm := NewLockManager(nil)

	// Create a channel to track events
	events := make(chan string, 10)

	// Simulate Node 1 acquiring a lock but "losing" the response
	go func() {
		events <- "Node1-Attempting"

		// Force the server to think Node 1 has the lock
		lm.ForceSetLockHolder(1)
		events <- "Server-GrantedLock-ResponseLost"

		// Wait a bit to simulate timeout
		time.Sleep(500 * time.Millisecond)

		// Node 1 retries the lock acquisition (should succeed immediately)
		events <- "Node1-Retrying"
		success, token := lm.AcquireWithTimeout(1, context.Background())

		if !success {
			t.Error("Node 1 retry should succeed because it already holds the lock")
		}
		events <- "Node1-RetrySucceeded"

		// Hold the lock for a bit
		time.Sleep(200 * time.Millisecond)

		// Node 1 releases the lock
		if !lm.Release(1, token) {
			t.Error("Node 1 should be able to release the lock")
		}
		events <- "Node1-ReleasedLock"
	}()

	// Wait for events from Node 1
	expectEvent(t, events, "Node1-Attempting")
	expectEvent(t, events, "Server-GrantedLock-ResponseLost")

	// Start Node 2 to wait for the lock
	node2Acquired := make(chan bool)
	go func() {
		events <- "Node2-WaitingForLock"

		// Try to acquire the lock - should block until Node 1 releases it
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		success, token := lm.AcquireWithTimeout(2, ctx)

		if !success {
			t.Error("Node 2 should eventually acquire the lock")
		}

		events <- "Node2-AcquiredLock"
		node2Acquired <- true

		// Hold the lock for a bit
		time.Sleep(100 * time.Millisecond)

		// Release the lock
		lm.Release(2, token)
		events <- "Node2-ReleasedLock"
	}()

	// Wait for Node 2 to start waiting
	expectEvent(t, events, "Node2-WaitingForLock")

	// Wait for the rest of the events
	expectEvent(t, events, "Node1-Retrying")
	expectEvent(t, events, "Node1-RetrySucceeded")
	expectEvent(t, events, "Node1-ReleasedLock")

	// Verify Node 2 got the lock
	select {
	case <-node2Acquired:
		// Expected
	case <-time.After(1 * time.Second):
		t.Error("Node 2 should have acquired the lock after Node 1 released it")
	}

	// Final events
	expectEvent(t, events, "Node2-AcquiredLock")
	expectEvent(t, events, "Node2-ReleasedLock")
}

// Helper function to verify events occur in expected order with a specific timeout
func expectEvent(t *testing.T, events chan string, expected string, timeout ...time.Duration) {
	select {
	case event := <-events:
		if event != expected {
			t.Errorf("Expected event %s, got %s", expected, event)
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for event: %s", expected)
	}
}

func TestLeaseExpiration(t *testing.T) {
	// Create a lock manager with a short lease duration for testing
	lm := NewLockManagerWithLeaseDuration(nil, 2*time.Second)

	// Test 1: Basic lease expiration
	t.Run("Basic lease expiration", func(t *testing.T) {
		// Acquire lock for client 1
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Verify lock is held
		if !lm.HasLockWithToken(1, token) {
			t.Fatal("Lock should be held by client 1")
		}

		// Wait for lease to expire
		time.Sleep(3 * time.Second)

		// Verify lock is released
		if lm.HasLockWithToken(1, token) {
			t.Fatal("Lock should be released after lease expiration")
		}
	})

	// Test 2: Multiple clients with lease expiration
	t.Run("Multiple clients with lease expiration", func(t *testing.T) {
		// Client 1 acquires lock
		success1, token1 := lm.Acquire(1)
		if !success1 {
			t.Fatal("Failed to acquire lock for client 1")
		}

		// Client 2 tries to acquire lock (should wait)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		success2, _ := lm.AcquireWithTimeout(2, ctx)
		cancel()
		if success2 {
			t.Fatal("Client 2 should not acquire lock while client 1 holds it")
		}

		// Wait for client 1's lease to expire
		time.Sleep(3 * time.Second)

		// Client 2 should now be able to acquire the lock
		success2, token2 := lm.Acquire(2)
		if !success2 {
			t.Fatal("Client 2 should acquire lock after client 1's lease expires")
		}

		// Verify client 1's token is no longer valid
		if lm.HasLockWithToken(1, token1) {
			t.Fatal("Client 1's lock should be invalid after lease expiration")
		}

		// Verify client 2's token is valid
		if !lm.HasLockWithToken(2, token2) {
			t.Fatal("Client 2's lock should be valid")
		}

		// Clean up
		lm.Release(2, token2)
	})

	// Test 3: Lease renewal
	t.Run("Lease renewal", func(t *testing.T) {
		// Client 1 acquires lock
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Wait half the lease duration
		time.Sleep(1 * time.Second)

		// Renew the lease
		if !lm.RenewLease(1, token) {
			t.Fatal("Failed to renew lease")
		}

		// Wait for a short time (less than the lease duration)
		time.Sleep(500 * time.Millisecond)

		// Lock should still be valid due to renewal
		if !lm.HasLockWithToken(1, token) {
			t.Fatal("Lock should still be valid after lease renewal")
		}

		// Clean up - explicitly release the lock
		if !lm.Release(1, token) {
			t.Fatal("Failed to release lock during cleanup")
		}

		// Verify lock is actually released
		if lm.HasLockWithToken(1, token) {
			t.Fatal("Lock should be released after cleanup")
		}
	})

	// Test 4: Client crash simulation
	t.Run("Client crash simulation", func(t *testing.T) {
		// Client 1 acquires lock
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Simulate client crash by not renewing lease
		// Wait for lease to expire
		time.Sleep(3 * time.Second)

		// Verify lock is released
		if lm.HasLockWithToken(1, token) {
			t.Fatal("Lock should be released after client crash")
		}

		// Another client should be able to acquire the lock
		success2, token2 := lm.Acquire(2)
		if !success2 {
			t.Fatal("Client 2 should be able to acquire lock after client 1 crashes")
		}

		// Verify client 2's lock is valid
		if !lm.HasLockWithToken(2, token2) {
			t.Fatal("Client 2's lock should be valid")
		}

		// Clean up
		lm.Release(2, token2)
	})
}

func TestLeaseExpirationWithFileOperations(t *testing.T) {
	// Create a lock manager with a short lease duration
	lm := NewLockManagerWithLeaseDuration(nil, 2*time.Second)

	// Test: File operations after lease expiration
	t.Run("File operations after lease expiration", func(t *testing.T) {
		// Client 1 acquires lock
		success, token := lm.Acquire(1)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Wait for lease to expire
		time.Sleep(3 * time.Second)

		// Try to perform file operation with expired lease
		if lm.HasLockWithToken(1, token) {
			t.Fatal("File operations should not be allowed with expired lease")
		}

		// Another client should be able to acquire the lock
		success2, token2 := lm.Acquire(2)
		if !success2 {
			t.Fatal("Client 2 should be able to acquire lock after client 1's lease expires")
		}

		// Verify client 2 can perform file operations
		if !lm.HasLockWithToken(2, token2) {
			t.Fatal("Client 2 should be able to perform file operations with valid lease")
		}
	})
}
