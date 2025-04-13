package file_manager

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"Distributed-Lock-Manager/internal/lock_manager"
	"Distributed-Lock-Manager/internal/wal"
)

// FileOperation represents a file operation performed with a specific token
type FileOperation struct {
	Filename  string
	StartPos  int64
	DataSize  int64
	Timestamp time.Time
}

// FileManager handles all file-related operations
type FileManager struct {
	openFiles          map[string]*os.File // Tracks open file handles
	fileLocks          map[string]string   // maps filename to lock token
	mu                 sync.RWMutex        // Protects maps
	logger             *log.Logger
	syncEnabled        bool                      // Toggle for fsync after writes
	wal                *wal.WriteAheadLog        // Write-ahead log for crash recovery
	lockManager        *lock_manager.LockManager // Reference to lock manager for token validation
	recoveryDone       bool                      // Indicates if WAL recovery is complete
	recoveryErr        error                     // Stores any error during recovery
	processedRequests  map[string]bool           // Set of request IDs that have been processed
	processedRequestFP *os.File                  // File pointer for processed requests log

	// Track operations by token for potential rollback
	tokenOperations map[string][]FileOperation // Maps token to operations performed with it
}

// NewFileManager initializes a new file manager
func NewFileManager(syncEnabled bool, lockManager *lock_manager.LockManager) *FileManager {
	return NewFileManagerWithWAL(syncEnabled, false, lockManager)
}

// NewFileManagerWithWAL initializes a new file manager with optional write-ahead logging
func NewFileManagerWithWAL(syncEnabled bool, walEnabled bool, lockManager *lock_manager.LockManager) *FileManager {
	logger := log.New(os.Stdout, "[FileManager] ", log.LstdFlags)

	// Initialize the write-ahead log
	wal, err := wal.NewWriteAheadLog(walEnabled)
	if err != nil {
		logger.Printf("Warning: Failed to initialize write-ahead log: %v", err)
		logger.Printf("Continuing without write-ahead logging")
	}

	fm := &FileManager{
		openFiles:         make(map[string]*os.File),
		fileLocks:         make(map[string]string),
		logger:            logger,
		syncEnabled:       syncEnabled,
		wal:               wal,
		lockManager:       lockManager,
		recoveryDone:      false,
		recoveryErr:       nil,
		processedRequests: make(map[string]bool),
		tokenOperations:   make(map[string][]FileOperation),
	}

	// Ensure the data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		logger.Printf("Warning: Failed to create data directory: %v", err)
	}

	// Initialize processed requests log
	if err := fm.initProcessedRequestsLog(); err != nil {
		logger.Printf("Warning: Failed to initialize processed requests log: %v", err)
	}

	// If WAL is enabled, perform recovery on startup
	if walEnabled && wal != nil {
		if err := fm.recoverFromWAL(); err != nil {
			fm.logger.Printf("Warning: Error recovering from write-ahead log: %v", err)
			fm.recoveryErr = err
		} else {
			fm.logger.Printf("Successfully recovered from write-ahead log")
		}
		fm.recoveryDone = true
	} else {
		// If WAL is not enabled, mark recovery as done
		fm.recoveryDone = true
	}

	return fm
}

// initProcessedRequestsLog opens the processed requests log file
func (fm *FileManager) initProcessedRequestsLog() error {
	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		return fmt.Errorf("couldn't create data directory: %v", err)
	}

	// Open the processed requests log file
	processedRequestsPath := filepath.Join("data", "processed_requests.log")
	fp, err := os.OpenFile(processedRequestsPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("couldn't open processed requests log: %v", err)
	}

	fm.processedRequestFP = fp

	// Load existing processed requests
	return fm.loadProcessedRequests()
}

// loadProcessedRequests reads the processed requests log file and populates the processedRequests map
func (fm *FileManager) loadProcessedRequests() error {
	if fm.processedRequestFP == nil {
		return fmt.Errorf("processed requests log file not initialized")
	}

	// Reset file pointer to beginning
	if _, err := fm.processedRequestFP.Seek(0, 0); err != nil {
		return fmt.Errorf("couldn't seek in processed requests log: %v", err)
	}

	// Read the file line by line
	scanner := bufio.NewScanner(fm.processedRequestFP)
	loadedCount := 0

	fm.mu.Lock()
	defer fm.mu.Unlock()

	for scanner.Scan() {
		requestID := strings.TrimSpace(scanner.Text())
		if requestID != "" {
			fm.processedRequests[requestID] = true
			loadedCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading processed requests log: %v", err)
	}

	fm.logger.Printf("Loaded %d processed request IDs from log", loadedCount)
	return nil
}

// recordProcessedRequest adds a request ID to the processed requests log
func (fm *FileManager) recordProcessedRequest(requestID string) error {
	if requestID == "" {
		return nil // Skip empty request IDs
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Check if already processed to avoid duplicates
	if fm.processedRequests[requestID] {
		return nil
	}

	// Add to in-memory map
	fm.processedRequests[requestID] = true

	// Write to log file if available
	if fm.processedRequestFP != nil {
		if _, err := fm.processedRequestFP.WriteString(requestID + "\n"); err != nil {
			return fmt.Errorf("failed to write to processed requests log: %v", err)
		}

		// Sync to ensure durability
		if err := fm.processedRequestFP.Sync(); err != nil {
			return fmt.Errorf("failed to sync processed requests log: %v", err)
		}
	}

	return nil
}

// isAlreadyProcessed checks if a request has already been processed
func (fm *FileManager) isAlreadyProcessed(requestID string) bool {
	if requestID == "" {
		return false
	}

	fm.mu.RLock()
	defer fm.mu.RUnlock()

	return fm.processedRequests[requestID]
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

// recoverFromWAL attempts to recover any operations that were committed in WAL but might not have been completed
func (fm *FileManager) recoverFromWAL() error {
	fm.logger.Printf("Attempting to recover from write-ahead log")

	// Load the set of requests known to have completed their file write
	if err := fm.loadProcessedRequests(); err != nil {
		fm.logger.Printf("Warning: Failed to load processed requests: %v", err)
		// Continue anyway, but recovery might do redundant work
	}

	fm.mu.RLock()
	processedCount := len(fm.processedRequests)
	fm.mu.RUnlock()
	fm.logger.Printf("Loaded %d already completed request IDs", processedCount)

	// Find operations that were marked as committed in the WAL
	// These are candidates for replay *only if* they aren't in processedRequests
	committedOps, err := wal.RecoverUncommittedOperations("logs")
	if err != nil {
		return fmt.Errorf("failed to recover committed operations from WAL: %v", err)
	}

	fm.logger.Printf("Found %d WAL-committed operations to check against processed list", len(committedOps))

	skipped := 0
	replayed := 0

	// Replay operations that were committed in WAL but *not* yet recorded as processed
	for _, entry := range committedOps {
		// Check the persistent processed list
		if fm.isAlreadyProcessed(entry.RequestID) {
			fm.logger.Printf("Skipping replay for request=%s (already processed before crash)", entry.RequestID)
			skipped++
			continue // Already done
		}

		// If we are here, the COMMIT marker made it to WAL, but the processedRequests log
		// write didn't (or the file write itself failed before that). We MUST retry the file write.
		fm.logger.Printf("Replaying file write for WAL-committed but unprocessed request=%s, file=%s", entry.RequestID, entry.Filename)
		replayed++

		// Perform the file append again (idempotency relies on this replay if needed)
		if err := fm.appendToFileInternal(entry.Filename, entry.Content, true); err != nil {
			fm.logger.Printf("ERROR: Failed to replay file write for request=%s: %v", entry.RequestID, err)
			// This is a more serious error - recovery couldn't complete a committed op.
			// Potentially continue with others, but log failure prominently.
			if fm.recoveryErr == nil {
				fm.recoveryErr = fmt.Errorf("failed replay for %s: %w", entry.RequestID, err) // Record first error
			}
			continue
		}

		// Now that replay succeeded, record it as processed durably
		if err := fm.recordProcessedRequest(entry.RequestID); err != nil {
			fm.logger.Printf("WARNING: Failed to record processed request after replay: %v", err)
		}
		// The primary WAL commit marker already exists, no need to call MarkCommitted again.
	}

	fm.logger.Printf("WAL recovery check completed: %d operations replayed, %d skipped", replayed, skipped)

	return fm.recoveryErr // Return nil or the first error encountered during replay
}

// AppendToFile appends content to a file, with optional WAL logging
func (fm *FileManager) AppendToFile(filename string, content []byte, clientID int32, token string) error {
	// Generate a request ID if none is provided
	requestID := fmt.Sprintf("append_%d_%d", clientID, time.Now().UnixNano())
	return fm.AppendToFileWithRequestID(filename, content, requestID, clientID, token)
}

// AppendToFileWithRequestID appends content to a file with request ID tracking
func (fm *FileManager) AppendToFileWithRequestID(filename string, content []byte, requestID string, clientID int32, token string) error {
	fm.logger.Printf("Attempting to append to %s (request ID: %s, client: %d)", filename, requestID, clientID)

	// --- Idempotency Check (using persistent processedRequests) ---
	if fm.isAlreadyProcessed(requestID) {
		fm.logger.Printf("Skipping duplicate request %s - already processed", requestID)
		return nil
	}

	// --- Validate Token ---
	if err := fm.validateToken(clientID, token); err != nil {
		fm.logger.Printf("File append failed: %v", err)
		return err
	}

	// --- Step 1: Log Intent (if WAL enabled) ---
	if fm.wal != nil {
		if err := fm.wal.LogOperation(requestID, filename, content); err != nil {
			fm.logger.Printf("ERROR: Failed to log operation intent to WAL: %v", err)
			// Fail immediately if intent cannot be logged
			return fmt.Errorf("failed to log operation intent to WAL: %w", err)
		}
	}

	// --- Step 2: Write Commit Marker to WAL (if WAL enabled) ---
	// This indicates the operation *will* be performed or *has been* performed.
	if fm.wal != nil {
		if err := fm.wal.MarkCommitted(requestID); err != nil {
			fm.logger.Printf("ERROR: Failed to write commit marker to WAL: %v", err)
			// Fail if commit marker cannot be written durably - we cannot guarantee atomicity
			return fmt.Errorf("failed to write commit marker to WAL: %w", err)
		}
		// If this sync succeeds, we are committed to performing the file write,
		// even if the server crashes immediately after.
	}

	// Get the current file size before appending (for potential rollback)
	fullPath := filepath.Join("data", filename)
	var startPos int64 = 0

	if fileInfo, err := os.Stat(fullPath); err == nil {
		startPos = fileInfo.Size()
	}

	// --- Step 3: Perform the Actual File Append ---
	// If this fails, the commit marker is already in the WAL. Recovery will handle it.
	if err := fm.appendToFileInternal(filename, content, true); err != nil {
		fm.logger.Printf("ERROR: File append failed after commit marker write: %v", err)
		// Operation is "committed" in WAL, but failed locally.
		// Recovery *should* replay this based on the commit marker.
		// Return the error, but the state is recoverable.
		return err
	}

	// Record this operation for potential rollback
	fm.mu.Lock()
	operation := FileOperation{
		Filename:  filename,
		StartPos:  startPos,
		DataSize:  int64(len(content)),
		Timestamp: time.Now(),
	}
	fm.tokenOperations[token] = append(fm.tokenOperations[token], operation)
	fm.mu.Unlock()

	// --- Step 4: Record as Processed (for runtime idempotency and skipping replay) ---
	// Now that the file write has succeeded, record it durably.
	if err := fm.recordProcessedRequest(requestID); err != nil {
		fm.logger.Printf("WARNING: Failed to record processed request: %v", err)
		// Continue anyway - this is not critical as WAL has the commit marker
	}

	// Success
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

	// Lock this specific file for writing
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
		filename := fmt.Sprintf("file_%d", i)
		// Create file only if it doesn't exist
		if _, err := os.Stat(filepath.Join("data", filename)); os.IsNotExist(err) {
			// Log the file creation operation if WAL is enabled
			if fm.wal != nil {
				requestID := fmt.Sprintf("create_file_%d", i)
				if err := fm.wal.LogOperation(requestID, filename, []byte{}); err != nil {
					fm.logger.Printf("Warning: Failed to log file creation to WAL: %v", err)
				}
			}

			f, err := os.Create(filepath.Join("data", filename))
			if err != nil {
				fm.logger.Fatalf("Failed to create file %s: %v", filename, err)
			}
			f.Close()

			// Mark the operation as committed if WAL is enabled
			if fm.wal != nil {
				requestID := fmt.Sprintf("create_file_%d", i)
				if err := fm.wal.MarkCommitted(requestID); err != nil {
					fm.logger.Printf("Warning: Failed to mark file creation as committed: %v", err)
				}
			}

			fm.logger.Printf("Created file: %s", filename)
		}
	}
}

// Cleanup closes all open files and cleans up resources
func (fm *FileManager) Cleanup() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Close all open files
	for _, f := range fm.openFiles {
		f.Close()
	}
	fm.openFiles = make(map[string]*os.File)

	// Close the WAL if it exists
	if fm.wal != nil {
		if err := fm.wal.Close(); err != nil {
			fm.logger.Printf("Warning: Failed to close WAL: %v", err)
		}
	}

	// Close the processed requests log file
	if fm.processedRequestFP != nil {
		if err := fm.processedRequestFP.Close(); err != nil {
			fm.logger.Printf("Warning: Failed to close processed requests log: %v", err)
		}
		fm.processedRequestFP = nil
	}
}

// ReadFile reads the entire content of a file
func (fm *FileManager) ReadFile(filename string) ([]byte, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	file, exists := fm.openFiles[filename]
	if !exists {
		filepath := filepath.Join("data", filename)
		var err error
		file, err = os.Open(filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", filename, err)
		}
		fm.openFiles[filename] = file
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", filename, err)
	}

	// Reset file pointer for future reads
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to reset file pointer: %v", err)
	}

	return content, nil
}

// IsRecoveryComplete returns true if WAL recovery has completed
func (fm *FileManager) IsRecoveryComplete() bool {
	return fm.recoveryDone
}

// GetRecoveryError returns any error that occurred during WAL recovery
func (fm *FileManager) GetRecoveryError() error {
	return fm.recoveryErr
}

// ClearProcessedRequests clears the processed requests list (for testing only)
func (fm *FileManager) ClearProcessedRequests() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Clear the in-memory map
	fm.processedRequests = make(map[string]bool)

	// Reset the processed requests log file
	if fm.processedRequestFP != nil {
		if err := fm.processedRequestFP.Close(); err != nil {
			return fmt.Errorf("failed to close processed requests log: %v", err)
		}
	}

	// Delete and recreate the file
	processedRequestsPath := filepath.Join("data", "processed_requests.log")
	if err := os.Remove(processedRequestsPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove processed requests log: %v", err)
	}

	// Reopen the file
	fp, err := os.OpenFile(processedRequestsPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("couldn't open processed requests log: %v", err)
	}

	fm.processedRequestFP = fp
	return nil
}

// RollbackTokenOperations removes all file operations performed with the given token
func (fm *FileManager) RollbackTokenOperations(token string) error {
	fm.mu.Lock()
	operations, exists := fm.tokenOperations[token]
	if !exists || len(operations) == 0 {
		fm.mu.Unlock()
		return nil // No operations to roll back
	}

	// Make a copy and remove from the map before unlocking
	opsCopy := make([]FileOperation, len(operations))
	copy(opsCopy, operations)
	delete(fm.tokenOperations, token)
	fm.mu.Unlock()

	fm.logger.Printf("Rolling back %d operations for token %s", len(opsCopy), token)

	// Process operations in reverse order (most recent first)
	for i := len(opsCopy) - 1; i >= 0; i-- {
		op := opsCopy[i]
		if err := fm.rollbackFileOperation(op); err != nil {
			fm.logger.Printf("Error rolling back operation on %s: %v", op.Filename, err)
			return err
		}
	}

	return nil
}

// rollbackFileOperation truncates a file to remove an appended segment
func (fm *FileManager) rollbackFileOperation(op FileOperation) error {
	fullPath := filepath.Join("data", op.Filename)

	// Open the file for reading and writing
	file, err := os.OpenFile(fullPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for rollback: %w", err)
	}
	defer file.Close()

	// Get current file size
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats for rollback: %w", err)
	}

	currentSize := info.Size()

	// Calculate expected size after operation
	expectedSize := op.StartPos + op.DataSize

	// Only truncate if the file size matches our expectation
	// This prevents accidental truncation if something else modified the file
	if currentSize >= expectedSize {
		// Truncate the file to remove this operation
		if err := file.Truncate(op.StartPos); err != nil {
			return fmt.Errorf("failed to truncate file: %w", err)
		}

		// Ensure data is synced to disk
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file after truncate: %w", err)
		}

		fm.logger.Printf("Successfully rolled back append operation on %s (removed %d bytes)",
			op.Filename, op.DataSize)
	} else {
		fm.logger.Printf("Skipping rollback for %s: file size (%d) doesn't match expected size (%d)",
			op.Filename, currentSize, expectedSize)
	}

	return nil
}

// RegisterForLeaseExpiry registers with the lock manager to be notified of lease expiries
func (fm *FileManager) RegisterForLeaseExpiry() {
	if fm.lockManager == nil {
		fm.logger.Printf("Warning: Cannot register for lease expiry notifications - no lock manager")
		return
	}

	// Setup a callback for when leases expire
	fm.lockManager.RegisterLeaseExpiryCallback(func(clientID int32, token string) {
		fm.logger.Printf("Lease expired for client %d with token %s, rolling back operations",
			clientID, token)

		if err := fm.RollbackTokenOperations(token); err != nil {
			fm.logger.Printf("Error during rollback for token %s: %v", token, err)
		}
	})
}
