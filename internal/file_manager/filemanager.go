package file_manager

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"Distributed-Lock-Manager/internal/lock_manager"
)

// FileManager handles all file-related operations
type FileManager struct {
	openFiles   map[string]*os.File    // Tracks open file handles
	fileLocks   map[string]*sync.Mutex // Per-file mutexes for concurrency
	mu          sync.Mutex             // Protects maps
	logger      *log.Logger
	syncEnabled bool                      // Toggle for fsync after writes
	wal         *WriteAheadLog            // Write-ahead log for crash recovery
	lockManager *lock_manager.LockManager // Reference to lock manager for token validation
}

// NewFileManager initializes a new file manager
func NewFileManager(syncEnabled bool, lockManager *lock_manager.LockManager) *FileManager {
	return NewFileManagerWithWAL(syncEnabled, false, lockManager)
}

// NewFileManagerWithWAL initializes a new file manager with optional write-ahead logging
func NewFileManagerWithWAL(syncEnabled bool, walEnabled bool, lockManager *lock_manager.LockManager) *FileManager {
	logger := log.New(os.Stdout, "[FileManager] ", log.LstdFlags)

	// Initialize the write-ahead log
	wal, err := NewWriteAheadLog(walEnabled)
	if err != nil {
		logger.Printf("Warning: Failed to initialize write-ahead log: %v", err)
		logger.Printf("Continuing without write-ahead logging")
	}

	fm := &FileManager{
		openFiles:   make(map[string]*os.File),
		fileLocks:   make(map[string]*sync.Mutex),
		logger:      logger,
		syncEnabled: syncEnabled,
		wal:         wal,
		lockManager: lockManager,
	}

	// If WAL is enabled, perform recovery on startup
	if walEnabled && wal != nil {
		if err := fm.recoverFromWAL(); err != nil {
			logger.Printf("Warning: Error recovering from write-ahead log: %v", err)
		}
	}

	return fm
}

// validateToken checks if the client has permission to modify files
func (fm *FileManager) validateToken(clientID int32, token string) error {
	if fm.lockManager == nil {
		return fmt.Errorf("lock manager not initialized")
	}

	if !fm.lockManager.HasLockWithToken(clientID, token) {
		return fmt.Errorf("unauthorized access: client %d does not have a valid token", clientID)
	}

	return nil
}

// recoverFromWAL attempts to recover any uncommitted operations from the write-ahead log
func (fm *FileManager) recoverFromWAL() error {
	fm.logger.Printf("Attempting to recover from write-ahead log")

	// Find uncommitted operations
	uncommitted, err := RecoverUncommittedOperations("logs")
	if err != nil {
		return fmt.Errorf("failed to recover operations: %v", err)
	}

	fm.logger.Printf("Found %d uncommitted operations", len(uncommitted))

	// Replay each uncommitted operation
	for _, entry := range uncommitted {
		fm.logger.Printf("Replaying operation: request=%s, file=%s", entry.RequestID, entry.Filename)

		// During recovery, we don't validate tokens since the lock state
		// might not be fully recovered yet. The lock manager will handle
		// any inconsistencies when it recovers its own state.
		if err := fm.appendToFileInternal(entry.Filename, entry.Content, false); err != nil {
			fm.logger.Printf("Error replaying operation: %v", err)
			// Continue with other operations even if one fails
			continue
		}

		// Mark the operation as committed
		if fm.wal != nil {
			if err := fm.wal.MarkCommitted(entry.RequestID); err != nil {
				fm.logger.Printf("Warning: Failed to mark operation as committed: %v", err)
			}
		}
	}

	return nil
}

// AppendToFile appends content to a file, with optional WAL logging
func (fm *FileManager) AppendToFile(filename string, content []byte, clientID int32, token string) error {
	return fm.AppendToFileWithRequestID(filename, content, "", clientID, token)
}

// AppendToFileWithRequestID appends content to a file with request ID tracking
func (fm *FileManager) AppendToFileWithRequestID(filename string, content []byte, requestID string, clientID int32, token string) error {
	fm.logger.Printf("Attempting to append to %s (request ID: %s, client: %d)", filename, requestID, clientID)

	// Validate token and permissions
	if err := fm.validateToken(clientID, token); err != nil {
		fm.logger.Printf("File append failed: %v", err)
		return err
	}

	// If WAL is enabled and we have a request ID, log the operation
	if fm.wal != nil && requestID != "" {
		if err := fm.wal.LogOperation(requestID, filename, content); err != nil {
			fm.logger.Printf("Warning: Failed to log operation to WAL: %v", err)
			// Continue with the operation even if logging fails
		}
	}

	// Perform the actual file append
	if err := fm.appendToFileInternal(filename, content, true); err != nil {
		return err
	}

	// If WAL is enabled and we have a request ID, mark the operation as committed
	if fm.wal != nil && requestID != "" {
		if err := fm.wal.MarkCommitted(requestID); err != nil {
			fm.logger.Printf("Warning: Failed to mark operation as committed: %v", err)
			// Operation was successful, so return success even if commit marker fails
		}
	}

	return nil
}

// appendToFileInternal is the internal implementation of file append
// The logOperation flag controls whether the operation should be logged to the WAL
func (fm *FileManager) appendToFileInternal(filename string, content []byte, forceSync bool) error {
	// Validate filename (must be "file_0" to "file_99")
	if !strings.HasPrefix(filename, "file_") {
		fm.logger.Printf("File append failed: invalid filename format %s", filename)
		return fmt.Errorf("invalid filename format")
	}

	numStr := strings.TrimPrefix(filename, "file_")
	num, err := strconv.Atoi(numStr)
	if err != nil || num < 0 || num >= 100 {
		fm.logger.Printf("File append failed: invalid file number %s", numStr)
		return fmt.Errorf("invalid file number")
	}

	// Prepend "data/" to the filename
	fullPath := filepath.Join("data", filename)

	// Ensure the data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		fm.logger.Printf("File append failed: couldn't create data directory: %v", err)
		return err
	}

	// Get or create a mutex for this file
	fm.mu.Lock()
	if _, exists := fm.fileLocks[fullPath]; !exists {
		fm.fileLocks[fullPath] = &sync.Mutex{}
	}
	fileMutex := fm.fileLocks[fullPath]
	fm.mu.Unlock()

	// Lock this specific file for writing
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// Get or open the file
	var f *os.File
	fm.mu.Lock()
	f, exists := fm.openFiles[fullPath]
	if !exists {
		// Create the file if it doesn't exist
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			fm.logger.Printf("Creating new file: %s", fullPath)
			f, err = os.Create(fullPath)
			if err != nil {
				fm.mu.Unlock()
				fm.logger.Printf("File append failed: couldn't create file: %v", err)
				return err
			}
		} else {
			// Open existing file
			f, err = os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fm.mu.Unlock()
				fm.logger.Printf("File append failed: couldn't open file: %v", err)
				return err
			}
		}
		fm.openFiles[fullPath] = f
	}
	fm.mu.Unlock()

	// Append content to the file
	_, err = f.Write(content)
	if err != nil {
		fm.logger.Printf("File append failed: couldn't write to file: %v", err)
		return err
	}

	// Ensure data is written to disk if enabled or forced
	if fm.syncEnabled || forceSync {
		if err := f.Sync(); err != nil {
			fm.logger.Printf("File append warning: couldn't sync file: %v", err)
		}
	}

	fm.logger.Printf("Successfully appended %d bytes to %s", len(content), fullPath)
	return nil
}

// CreateFiles ensures the 100 files exist
func (fm *FileManager) CreateFiles() {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll("data", 0755); err != nil {
		fm.logger.Fatalf("Failed to create data directory: %v", err)
	}

	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("data/file_%d", i)
		// Create file only if it doesn't exist
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			f, err := os.Create(filename)
			if err != nil {
				fm.logger.Fatalf("Failed to create file %s: %v", filename, err)
			}
			f.Close()
			fm.logger.Printf("Created file: %s", filename)
		}
	}

	fm.logger.Printf("All files created successfully")
}

// Cleanup closes any open files
func (fm *FileManager) Cleanup() {
	// Close all open file handles
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for name, file := range fm.openFiles {
		if err := file.Close(); err != nil {
			fm.logger.Printf("Error closing file %s: %v", name, err)
		}
		delete(fm.openFiles, name)
	}

	// Close the write-ahead log if it exists
	if fm.wal != nil {
		if err := fm.wal.Close(); err != nil {
			fm.logger.Printf("Error closing write-ahead log: %v", err)
		}
	}

	fm.logger.Println("File manager cleanup complete")
}
