package client

import (
	"fmt"
)

// ServerUnavailableError is defined in client.go

// InvalidTokenError represents an error when the token is invalid or expired
type InvalidTokenError struct {
	Message string
}

func (e *InvalidTokenError) Error() string {
	return fmt.Sprintf("invalid token: %s", e.Message)
}

// IsServerUnavailable is already defined in client.go

// IsInvalidToken checks if the error is an InvalidTokenError
func IsInvalidToken(err error) bool {
	_, ok := err.(*InvalidTokenError)
	return ok
}
