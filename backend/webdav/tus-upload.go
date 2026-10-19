package webdav

import (
	"encoding/base64"
	"fmt"
	"io"
	"strings"
)

// Metadata is a typedef for a string to string map to hold metadata
type Metadata map[string]string

// Upload is a struct containing the file status during upload
type Upload struct {
	// stream is read sequentially, one chunk at a time. It is never
	// rewound: the tus offset advances with every accepted PATCH and
	// resuming a previous upload is not implemented.
	stream io.Reader
	size   int64

	Metadata Metadata
}

// EncodedMetadata encodes the upload metadata.
func (u *Upload) EncodedMetadata() string {
	var encoded []string

	for k, v := range u.Metadata {
		encoded = append(encoded, fmt.Sprintf("%s %s", k, b64encode(v)))
	}

	return strings.Join(encoded, ",")
}

func b64encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// NewUpload creates a new upload from an io.Reader.
//
// The reader is consumed sequentially in chunk sized pieces, so it does
// not need to implement io.Seeker.
func NewUpload(reader io.Reader, size int64, metadata Metadata) *Upload {
	if metadata == nil {
		metadata = make(Metadata)
	}

	return &Upload{
		stream: reader,
		size:   size,

		Metadata: metadata,
	}
}
