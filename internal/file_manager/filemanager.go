package file_manager

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// FileManager handles all file-related operations
type FileManager struct {
	openFiles   map[string]*os.File    // Tracks open file handles
	fileLocks   map[string]*sync.Mutex // Per-file mutexes for concurrency
	mu          sync.Mutex             // Protects maps
	logger      *log.Logger
	syncEnabled bool // Toggle for fsync after writes
}

// NewFileManager initializes a new file manager
func NewFileManager(syncEnabled bool) *FileManager {
	return &FileManager{
		openFiles:   make(map[string]*os.File),
		fileLocks:   make(map[string]*sync.Mutex),
		logger:      log.New(os.Stdout, "[FileManager] ", log.LstdFlags),
		syncEnabled: syncEnabled,
	}
}

// AppendToFile appends content to a file
func (fm *FileManager) AppendToFile(filename string, content []byte) error {
	fm.logger.Printf("Attempting to append to %s", filename)

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

	// Ensure data is written to disk if enabled
	if fm.syncEnabled {
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

	fm.logger.Println("File manager cleanup complete")
}
