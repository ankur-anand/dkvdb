package api

import "errors"

// contains all the public API Errors.

var (
	// ErrInternalServer ...
	ErrInternalServer = errors.New("Internal Server Error")
	// ErrUnprocessableEntity ...
	ErrUnprocessableEntity = errors.New("Unprocessable Entity")
	// ErrIterFinished ...
	ErrIterFinished = errors.New("iterator finished")
)
