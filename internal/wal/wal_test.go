package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// cleanupLogs removes all WAL files from the logs directory
func cleanupLogs(t *testing.T) {
	logDir := "logs"
	if err := os.RemoveAll(logDir); err != nil {
		t.Fatalf("Failed to clean up logs directory: %v", err)
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("Failed to recreate logs directory: %v", err)
	}
}

func TestWriteAheadLogging(t *testing.T) {
	// Clean up any existing logs before starting the test
	cleanupLogs(t)

	// Create a test WAL
	wal, err := NewWriteAheadLog(true)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Test data
	testData := []byte("test data")
	filename := "file_0"
	requestID := "test_request_1"

	// Step 1: Log an operation
	t.Run("Log Operation", func(t *testing.T) {
		if err := wal.LogOperation(requestID, filename, testData); err != nil {
			t.Fatalf("Failed to log operation: %v", err)
		}

		// Verify the log file exists using OS-agnostic path
		logFiles, err := filepath.Glob(filepath.Join("logs", "wal-*.log"))
		if err != nil {
			t.Fatalf("Failed to find log files: %v", err)
		}
		if len(logFiles) == 0 {
			t.Fatal("No log files found")
		}
	})

	// Step 2: Mark operation as committed
	t.Run("Mark Committed", func(t *testing.T) {
		if err := wal.MarkCommitted(requestID); err != nil {
			t.Fatalf("Failed to mark operation as committed: %v", err)
		}

		// Verify the operation is marked as committed
		uncommitted, err := RecoverUncommittedOperations("logs")
		if err != nil {
			t.Fatalf("Failed to recover operations: %v", err)
		}
		if len(uncommitted) > 0 {
			t.Error("Found uncommitted operations after marking as committed")
		}
	})

	// Step 3: Test WAL cleanup
	t.Run("WAL Cleanup", func(t *testing.T) {
		// Create an old WAL file using OS-agnostic path
		oldLogPath := filepath.Join("logs", "wal-old.log")
		oldFile, err := os.Create(oldLogPath)
		if err != nil {
			t.Fatalf("Failed to create old log file: %v", err)
		}
		oldFile.Close()

		// Set the modification time to 25 hours ago
		oldTime := time.Now().Add(-25 * time.Hour)
		if err := os.Chtimes(oldLogPath, oldTime, oldTime); err != nil {
			t.Fatalf("Failed to set file times: %v", err)
		}

		// Create a new WAL to trigger cleanup
		newWAL, err := NewWriteAheadLog(true)
		if err != nil {
			t.Fatalf("Failed to create new WAL: %v", err)
		}
		newWAL.Close()

		// Verify the old file was cleaned up
		if _, err := os.Stat(oldLogPath); err == nil {
			t.Error("Old WAL file was not cleaned up")
		}
	})
}

func TestRecovery(t *testing.T) {
	// Clean up any existing logs before starting the test
	cleanupLogs(t)

	t.Log("Starting TestRecovery")

	// Create a test WAL
	wal, err := NewWriteAheadLog(true)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	t.Log("Created WAL successfully")

	// Log multiple operations without committing
	operations := []struct {
		requestID string
		filename  string
		data      []byte
	}{
		{"op1", "file_1", []byte("operation 1")},
		{"op2", "file_1", []byte("operation 2")},
		{"op3", "file_1", []byte("operation 3")},
	}

	t.Logf("Logging %d operations", len(operations))
	for _, op := range operations {
		if err := wal.LogOperation(op.requestID, op.filename, op.data); err != nil {
			t.Fatalf("Failed to log operation %s: %v", op.requestID, err)
		}
		t.Logf("Logged operation: %s", op.requestID)
	}

	// Recover uncommitted operations
	t.Log("Recovering uncommitted operations")
	uncommitted, err := RecoverUncommittedOperations("logs")
	if err != nil {
		t.Fatalf("Failed to recover operations: %v", err)
	}

	// Verify all operations were recovered
	t.Logf("Recovered %d uncommitted operations", len(uncommitted))
	if len(uncommitted) != len(operations) {
		t.Errorf("Expected %d uncommitted operations, got %d", len(operations), len(uncommitted))
	}

	// Verify operation contents
	t.Log("Verifying operation contents")
	for i, op := range operations {
		if uncommitted[i].RequestID != op.requestID {
			t.Errorf("RequestID mismatch at index %d. Expected: %s, Got: %s", i, op.requestID, uncommitted[i].RequestID)
		}
		if uncommitted[i].Filename != op.filename {
			t.Errorf("Filename mismatch at index %d. Expected: %s, Got: %s", i, op.filename, uncommitted[i].Filename)
		}
		if string(uncommitted[i].Content) != string(op.data) {
			t.Errorf("Content mismatch at index %d. Expected: %s, Got: %s", i, op.data, uncommitted[i].Content)
		}
	}

	t.Log("TestRecovery completed successfully")
}
