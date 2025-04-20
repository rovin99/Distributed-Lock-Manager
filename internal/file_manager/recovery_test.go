package file_manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"Distributed-Lock-Manager/internal/lock_manager"

	"github.com/google/uuid"
)

// setupRecoveryTest creates a test environment with unique directories
func setupRecoveryTest(t *testing.T) (*FileManager, *lock_manager.LockManager, func()) {
	// Create unique test directories
	testID := uuid.New().String()
	dataDir := filepath.Join(os.TempDir(), "recovery_test_data_"+testID)
	logDir := filepath.Join(os.TempDir(), "recovery_test_logs_"+testID)

	// Create directories
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	// Set environment variables
	oldDataDir := os.Getenv("DATA_DIR")
	oldLogDir := os.Getenv("WAL_LOG_DIR")
	os.Setenv("DATA_DIR", dataDir)
	os.Setenv("WAL_LOG_DIR", logDir)

	// Create lock manager with short lease duration for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 5*time.Second)

	// Create file manager with WAL enabled
	fm := NewFileManagerWithWAL(true, true, lm)

	// Cleanup function
	cleanup := func() {
		// Cleanup file manager
		fm.Cleanup()

		// Restore environment variables
		os.Setenv("DATA_DIR", oldDataDir)
		os.Setenv("WAL_LOG_DIR", oldLogDir)

		// Remove test directories
		os.RemoveAll(dataDir)
		os.RemoveAll(logDir)
	}

	return fm, lm, cleanup
}

// assertFileContent checks if the file content matches the expected string
func assertFileContent(t *testing.T, fm *FileManager, filename, expected string) {
	content, err := fm.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read file %s: %v", filename, err)
	}
	if string(content) != expected {
		t.Fatalf("File content mismatch. Got %s, want %s", string(content), expected)
	}
}

// TestScenarioA_Recovery_LockFreeBeforeCrash tests recovery when the crash happens after the lock is released
func TestScenarioA_Recovery_LockFreeBeforeCrash(t *testing.T) {
	// Setup test environment
	fm1, lm1, cleanup1 := setupRecoveryTest(t)
	defer cleanup1()

	clientID1 := int32(1)
	filename := "file_10"
	reqID_A1 := "reqA1"
	reqID_A2 := "reqA2"
	reqID_1 := "req1"

	// --- Pre-Crash Operations ---
	t.Log("Scenario A: Pre-crash operations")

	// Acquire lock for client 1
	success, token1 := lm1.Acquire(clientID1)
	if !success {
		t.Fatalf("Failed to acquire lock for C1")
	}

	// First append 'A'
	err := fm1.AppendToFileWithRequestID(filename, []byte("A"), reqID_A1, clientID1, token1)
	if err != nil {
		t.Fatalf("Failed to append 'A' (1st): %v", err)
	}
	assertFileContent(t, fm1, filename, "A")

	// Second append 'A'
	err = fm1.AppendToFileWithRequestID(filename, []byte("A"), reqID_A2, clientID1, token1)
	if err != nil {
		t.Fatalf("Failed to append 'A' (2nd): %v", err)
	}
	assertFileContent(t, fm1, filename, "AA")

	// Release lock
	if !lm1.Release(clientID1, token1) {
		t.Fatalf("Failed to release lock for C1")
	}

	// --- Simulate Crash ---
	t.Log("Scenario A: Simulating crash")
	fm1.Cleanup()

	// --- Recover ---
	t.Log("Scenario A: Recovering")
	// Create new lock manager (lock state is not persistent)
	lm2 := lock_manager.NewLockManagerWithLeaseDuration(nil, 5*time.Second)
	// Create new file manager that will recover from WAL
	fm2 := NewFileManagerWithWAL(true, true, lm2)
	defer fm2.Cleanup()

	// Verify recovery status
	if !fm2.IsRecoveryComplete() {
		t.Fatal("Recovery did not complete")
	}
	if err := fm2.GetRecoveryError(); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	t.Log("Scenario A: Recovery complete")

	// --- Post-Recovery Operations ---
	t.Log("Scenario A: Post-recovery operations")

	// Acquire lock again
	success, token2 := lm2.Acquire(clientID1)
	if !success {
		t.Fatalf("Failed to acquire lock for C1 post-recovery")
	}

	// Append '1'
	err = fm2.AppendToFileWithRequestID(filename, []byte("1"), reqID_1, clientID1, token2)
	if err != nil {
		t.Fatalf("Failed to append '1' post-recovery: %v", err)
	}

	// --- Verification ---
	t.Log("Scenario A: Verifying final state")
	assertFileContent(t, fm2, filename, "AA1")

	// Release lock
	if !lm2.Release(clientID1, token2) {
		t.Fatalf("Failed to release lock for C1 post-recovery")
	}
}

// TestScenarioB_Recovery_LockHeldDuringCrash tests recovery when the crash happens while a lock is held
func TestScenarioB_Recovery_LockHeldDuringCrash(t *testing.T) {
	// Setup test environment
	fm1, lm1, cleanup1 := setupRecoveryTest(t)
	defer cleanup1()

	clientID1 := int32(1)
	clientID2 := int32(2)
	filename := "file_20"
	reqID_A1 := "reqA1_scenB"
	reqID_B1 := "reqB1_scenB"
	reqID_B2 := "reqB2_scenB"
	reqID_A2 := "reqA2_scenB"

	// --- Pre-Crash Operations ---
	t.Log("Scenario B: Pre-crash operations (Client 1)")

	// Client 1 acquires lock and appends 'A'
	success, tokenC1_pre := lm1.Acquire(clientID1)
	if !success {
		t.Fatalf("Failed to acquire lock for C1")
	}
	err := fm1.AppendToFileWithRequestID(filename, []byte("A"), reqID_A1, clientID1, tokenC1_pre)
	if err != nil {
		t.Fatalf("Failed to append 'A': %v", err)
	}
	assertFileContent(t, fm1, filename, "A")
	if !lm1.Release(clientID1, tokenC1_pre) {
		t.Fatalf("Failed to release lock for C1")
	}

	t.Log("Scenario B: Pre-crash operations (Client 2)")

	// Client 2 acquires lock and appends 'B'
	success, tokenC2_pre := lm1.Acquire(clientID2)
	if !success {
		t.Fatalf("Failed to acquire lock for C2")
	}
	err = fm1.AppendToFileWithRequestID(filename, []byte("B"), reqID_B1, clientID2, tokenC2_pre)
	if err != nil {
		t.Fatalf("Failed to append 'B' (before crash): %v", err)
	}
	assertFileContent(t, fm1, filename, "AB")

	// --- Simulate Crash ---
	t.Log("Scenario B: Simulating crash")
	fm1.Cleanup()

	// --- Recover ---
	t.Log("Scenario B: Recovering")
	lm2 := lock_manager.NewLockManagerWithLeaseDuration(nil, 5*time.Second)
	fm2 := NewFileManagerWithWAL(true, true, lm2)
	defer fm2.Cleanup()

	if !fm2.IsRecoveryComplete() {
		t.Fatal("Recovery did not complete")
	}
	if err := fm2.GetRecoveryError(); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	t.Log("Scenario B: Recovery complete")

	// --- Post-Recovery Operations ---

	// Verify file state after recovery
	assertFileContent(t, fm2, filename, "AB")

	// Client 2 re-acquires lock and retries append 'B'
	t.Log("Scenario B: Post-recovery (Client 2 attempting idempotent append of B1)")
	success, tokenC2_post1 := lm2.Acquire(clientID2)
	if !success {
		t.Fatalf("Failed to re-acquire lock for C2 post-recovery")
	}
	err = fm2.AppendToFileWithRequestID(filename, []byte("B"), reqID_B1, clientID2, tokenC2_post1)
	if err != nil {
		t.Fatalf("Idempotent append of 'B' (reqB1) failed: %v", err)
	}
	assertFileContent(t, fm2, filename, "AB")
	if !lm2.Release(clientID2, tokenC2_post1) {
		t.Fatalf("Failed to release lock for C2 after idempotent check")
	}

	// Client 2 appends 'B' again with new request ID
	t.Log("Scenario B: Post-recovery (Client 2 appends 'B' again)")
	success, tokenC2_post2 := lm2.Acquire(clientID2)
	if !success {
		t.Fatalf("Failed to acquire lock for C2 for second B")
	}
	err = fm2.AppendToFileWithRequestID(filename, []byte("B"), reqID_B2, clientID2, tokenC2_post2)
	if err != nil {
		t.Fatalf("Failed to append 'B' (second time, post-recovery): %v", err)
	}
	assertFileContent(t, fm2, filename, "ABB")
	if !lm2.Release(clientID2, tokenC2_post2) {
		t.Fatalf("Failed to release lock for C2 after second B")
	}

	// Client 1 acquires lock and appends 'A'
	t.Log("Scenario B: Post-recovery (Client 1 acquires lock and appends 'A')")
	success, tokenC1_post := lm2.Acquire(clientID1)
	if !success {
		t.Fatalf("Failed to acquire lock for C1 post-recovery")
	}
	err = fm2.AppendToFileWithRequestID(filename, []byte("A"), reqID_A2, clientID1, tokenC1_post)
	if err != nil {
		t.Fatalf("Failed to append 'A' (post-recovery): %v", err)
	}

	// --- Verification ---
	t.Log("Scenario B: Verifying final state")
	assertFileContent(t, fm2, filename, "ABBA")

	if !lm2.Release(clientID1, tokenC1_post) {
		t.Fatalf("Failed to release lock for C1 post-recovery")
	}
}
