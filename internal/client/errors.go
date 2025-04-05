package client

import (
	"fmt"
)

// ServerUnavailableError represents an error when the server is unavailable after max retries
type ServerUnavailableError struct {
	Operation string
	Attempts  int
	LastError error
}

func (e *ServerUnavailableError) Error() string {
	return fmt.Sprintf("server unavailable: %s operation failed after %d attempts: %v",
		e.Operation, e.Attempts, e.LastError)
}

// InvalidTokenError represents an error when the token is invalid or expired
type InvalidTokenError struct {
	Message string
}

func (e *InvalidTokenError) Error() string {
	return fmt.Sprintf("invalid token: %s", e.Message)
}

// IsServerUnavailable checks if the error is a ServerUnavailableError
func IsServerUnavailable(err error) bool {
	_, ok := err.(*ServerUnavailableError)
	return ok
}

// IsInvalidToken checks if the error is an InvalidTokenError
func IsInvalidToken(err error) bool {
	_, ok := err.(*InvalidTokenError)
	return ok
}
