package replication

// Logger defines the interface for logging in the replication package
type Logger interface {
	// Infof logs an info message with format
	Infof(format string, args ...interface{})
	// Warnf logs a warning message with format
	Warnf(format string, args ...interface{})
	// Errorf logs an error message with format
	Errorf(format string, args ...interface{})
	// Debugf logs a debug message with format
	Debugf(format string, args ...interface{})
}
