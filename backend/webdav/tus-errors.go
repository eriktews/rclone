package webdav

import "errors"

var (
	// ErrChunkSize is returned when the chunk size is zero
	ErrChunkSize = errors.New("tus chunk size must be greater than zero")
	// ErrNilUpload is returned when the upload is nil
	ErrNilUpload = errors.New("tus upload can't be nil")
	// ErrLargeUpload is returned when the upload body is to large
	ErrLargeUpload = errors.New("tus upload body is to large")
	// ErrVersionMismatch is returned when the tus protocol version is mismatching
	ErrVersionMismatch = errors.New("tus protocol version mismatch")
	// ErrOffsetMismatch is returned when the tus upload offset is mismatching
	ErrOffsetMismatch = errors.New("tus upload offset mismatch")
)
