// wal_test.go
package wal

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteAheadLogging tests the basic WAL operations
func TestWriteAheadLogging(t *testing.T) {
	// Setup a temporary directory for tests
	testDir, err := ioutil.TempDir("", "wal-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Set up the logs directory environment variable to use our test directory
	originalLogsDir := os.Getenv("WAL_LOG_DIR")
	os.Setenv("WAL_LOG_DIR", testDir)
	defer os.Setenv("WAL_LOG_DIR", originalLogsDir)

	t.Run("Log Operation", func(t *testing.T) {
		logDir := filepath.Join(testDir, "log1")
		os.MkdirAll(logDir, 0755)

		// Override the logs directory for this test
		os.Setenv("WAL_LOG_DIR", logDir)

		wal, err := NewWriteAheadLog(true)
		assert.NoError(t, err)
		assert.NotNil(t, wal)

		err = wal.LogOperation("test_request_1", "file_0", []byte("test data"))
		assert.NoError(t, err)

		wal.Close()

		// Recover operations directly from logDir
		ops, err := RecoverUncommittedOperations(logDir)
		assert.NoError(t, err)
		// Since we didn't mark any operations as committed, no operations should be returned
		assert.Empty(t, ops, "Should find no operations to replay since none were committed")
	})

	t.Run("Mark Committed", func(t *testing.T) {
		logDir := filepath.Join(testDir, "log2")
		os.MkdirAll(logDir, 0755)

		// Override the logs directory for this test
		os.Setenv("WAL_LOG_DIR", logDir)

		wal, err := NewWriteAheadLog(true)
		require.NoError(t, err)
		require.NotNil(t, wal)

		// Log an operation
		err = wal.LogOperation("test_request_1", "file_0", []byte("test data"))
		require.NoError(t, err)

		// Mark it as committed
		err = wal.MarkCommitted("test_request_1")
		require.NoError(t, err)

		wal.Close()

		// Recover operations directly from logDir
		ops, err := RecoverUncommittedOperations(logDir)
		require.NoError(t, err)
		// With our new implementation, committed operations should be returned for potential replay
		require.Len(t, ops, 1, "Should find the committed operation for potential replay")
		assert.Equal(t, "test_request_1", ops[0].RequestID)
		assert.Equal(t, EntryTypeOperation, ops[0].Type)
	})

	t.Run("WAL Cleanup", func(t *testing.T) {
		logDir := filepath.Join(testDir, "log3")
		os.MkdirAll(logDir, 0755)

		// Override the logs directory for this test
		os.Setenv("WAL_LOG_DIR", logDir)

		// Create some old WAL files
		oldWalPath := filepath.Join(logDir, "wal-20060102-030405.log")
		err := ioutil.WriteFile(oldWalPath, []byte("test"), 0644)
		assert.NoError(t, err)

		// Set modification time to more than 24 hours ago
		oldTime := time.Now().Add(-25 * time.Hour)
		err = os.Chtimes(oldWalPath, oldTime, oldTime)
		assert.NoError(t, err)

		// Create a new WAL (should trigger cleanup)
		wal, err := NewWriteAheadLog(true)
		assert.NoError(t, err)
		wal.Close()

		// Old WAL file should be removed
		_, err = os.Stat(oldWalPath)
		assert.True(t, os.IsNotExist(err), "Old WAL file should be deleted")
	})
}

// TestRecovery tests recovering operations from WAL files
func TestRecovery(t *testing.T) {
	// Setup a temporary directory for tests
	testDir, err := ioutil.TempDir("", "wal-recovery-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Set up the logs directory environment variable to use our test directory
	originalLogsDir := os.Getenv("WAL_LOG_DIR")
	os.Setenv("WAL_LOG_DIR", testDir)
	defer os.Setenv("WAL_LOG_DIR", originalLogsDir)

	// Create a WAL
	wal, err := NewWriteAheadLog(true)
	require.NoError(t, err)

	// Log multiple operations
	err = wal.LogOperation("op1", "file_0", []byte("operation 1"))
	require.NoError(t, err)
	err = wal.LogOperation("op2", "file_1", []byte("operation 2"))
	require.NoError(t, err)
	err = wal.LogOperation("op3", "file_2", []byte("operation 3"))
	require.NoError(t, err)

	// Mark some as committed
	err = wal.MarkCommitted("op1")
	require.NoError(t, err)
	err = wal.MarkCommitted("op2")
	require.NoError(t, err)
	// op3 is left uncommitted

	wal.Close()

	// Recover operations directly from the temporary directory
	committedOps, err := RecoverUncommittedOperations(testDir)
	require.NoError(t, err)

	// With our new implementation, we should get the operations that were committed
	// but may not have completed file writes
	require.Len(t, committedOps, 2, "Expected 2 committed operations for replay")

	// Verify the operations
	var found1, found2 bool
	for _, op := range committedOps {
		assert.Equal(t, EntryTypeOperation, op.Type)
		if op.RequestID == "op1" {
			found1 = true
			assert.Equal(t, "file_0", op.Filename)
			assert.Equal(t, []byte("operation 1"), op.Content)
		} else if op.RequestID == "op2" {
			found2 = true
			assert.Equal(t, "file_1", op.Filename)
			assert.Equal(t, []byte("operation 2"), op.Content)
		}
	}

	assert.True(t, found1 && found2, "Should find both committed operations")
}

// TestWALChecksums tests the checksums functionality of the WAL
func TestWALChecksums(t *testing.T) {
	// Setup a temporary directory for tests
	testDir, err := ioutil.TempDir("", "wal-checksum-test")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// Set up the logs directory environment variable
	originalLogsDir := os.Getenv("WAL_LOG_DIR")
	os.Setenv("WAL_LOG_DIR", testDir)
	defer os.Setenv("WAL_LOG_DIR", originalLogsDir)

	t.Run("Checksum Creation and Validation", func(t *testing.T) {
		// Create a log entry
		entry := LogEntry{
			Timestamp: time.Now(),
			Type:      EntryTypeOperation,
			RequestID: "test_request_1",
			Filename:  "file.txt",
			Content:   []byte("test content"),
		}

		// Compute checksum
		entry.Checksum = entry.computeChecksum()

		// Verify checksum validation
		assert.True(t, entry.validateChecksum())

		// Corrupt the entry and verify checksum fails
		entry.Content = []byte("corrupted content")
		assert.False(t, entry.validateChecksum())
	})

	t.Run("WAL with Checksums", func(t *testing.T) {
		// Create a new WAL
		wal, err := NewWriteAheadLog(true)
		require.NoError(t, err)

		// Log some operations
		err = wal.LogOperation("req1", "file.txt", []byte("data1"))
		require.NoError(t, err)

		err = wal.MarkCommitted("req1")
		require.NoError(t, err)

		wal.Close()

		// Manually corrupt an entry in the WAL file
		files, err := filepath.Glob(filepath.Join(testDir, "wal-*.log"))
		require.NoError(t, err)
		require.Len(t, files, 1)

		// Read the file, replace the first entry's checksum
		data, err := ioutil.ReadFile(files[0])
		require.NoError(t, err)

		// Replace a portion of the checksum to corrupt it
		corruptedData := []byte(string(data))
		checksumIdx := bytes.Index(corruptedData, []byte("\"checksum\":"))
		require.NotEqual(t, -1, checksumIdx)
		// Find the actual checksum value
		checksumStart := bytes.Index(corruptedData[checksumIdx:], []byte("\"")) + checksumIdx + 1

		// Corrupt one character of the checksum
		corruptedData[checksumStart+5] = 'X'

		// Write back the corrupted file
		err = ioutil.WriteFile(files[0], corruptedData, 0644)
		require.NoError(t, err)

		// Now try to recover and verify the corrupt entry is skipped
		recoveredOps, err := RecoverUncommittedOperations(testDir)
		require.NoError(t, err)
		require.Empty(t, recoveredOps, "Corrupted entry should be skipped")
	})
}

// --- wal.go Modifications (If needed for testing) ---
// Consider modifying NewWriteAheadLog to accept logDir parameter
// func NewWriteAheadLog(enabled bool, logDir string) (*WriteAheadLog, error) { ... }
// And update RecoverUncommittedOperations if needed based on how logDir is passed.
