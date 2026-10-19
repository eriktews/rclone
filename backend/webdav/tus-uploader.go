package webdav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/rest"
)

// Uploader holds all information about a currently running upload
type Uploader struct {
	fs     *Fs
	url    string
	upload *Upload
	offset int64
	buf    []byte
}

func (f *Fs) shouldRetryChunk(ctx context.Context, resp *http.Response, err error, newOff *int64) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if resp != nil {
		switch resp.StatusCode {
		case 204:
			off, parseErr := strconv.ParseInt(resp.Header.Get("Upload-Offset"), 10, 64)
			if parseErr != nil {
				return false, parseErr
			}
			*newOff = off
			return false, nil

		case 409:
			return false, ErrOffsetMismatch
		case 412:
			return false, ErrVersionMismatch
		case 413:
			return false, ErrLargeUpload
		}
	}

	return f.shouldRetry(ctx, resp, err)
}

// canRecoverViaHead reports whether a failed chunk PATCH may still have
// been recorded by the server, so its offset should be queried with a
// HEAD request before resending the chunk.
func canRecoverViaHead(resp *http.Response, retry bool) bool {
	if !retry {
		return false
	}
	if resp == nil {
		return true
	}
	switch resp.StatusCode {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (u *Uploader) uploadChunk(ctx context.Context, data []byte, offset int64, options ...fs.OpenOption) (int64, error) {
	extraHeaders := map[string]string{} // FIXME: Use extraHeaders(ctx, src) from Object maybe?
	extraHeaders["Tus-Resumable"] = "1.0.0"
	extraHeaders["filetype"] = u.upload.Metadata["filetype"]

	wantOffset := offset + int64(len(data))
	curOffset := offset
	var size int64
	opts := rest.Opts{
		Method:        "PATCH",
		RootURL:       u.url,
		NoResponse:    true,
		ContentLength: &size,
		ContentType:   "application/offset+octet-stream",
		ExtraHeaders:  extraHeaders,
		Options:       options,
	}

	var newOffset int64
	for curOffset < wantOffset {
		progressed := false
		err := u.fs.pacer.Call(func() (bool, error) {
			progressed = false
			newOffset = curOffset
			rem := data[curOffset-offset:]
			size = int64(len(rem))
			extraHeaders["Upload-Offset"] = strconv.FormatInt(curOffset, 10)
			// The body must be an io.Closer so rest.Call waits for the
			// transport to stop reading it before the buffer is reused.
			opts.Body = io.NopCloser(bytes.NewReader(rem))
			opts.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(rem)), nil
			}
			res, err := u.fs.srv.Call(ctx, &opts)
			retry, err := u.fs.shouldRetryChunk(ctx, res, err, &newOffset)
			if err == nil {
				switch {
				case newOffset == wantOffset:
					curOffset = newOffset
					return false, nil
				case newOffset > curOffset && newOffset < wantOffset:
					// The server accepted only part of the chunk. Continue
					// with the rest of it.
					curOffset = newOffset
					progressed = true
					return true, nil
				case newOffset >= offset && newOffset < curOffset:
					// The server discarded part of the chunk, so rewind and
					// send it again.
					curOffset = newOffset
					return true, nil
				default:
					return false, ErrOffsetMismatch
				}
			}
			if ctx.Err() == nil && (canRecoverViaHead(res, retry) || errors.Is(err, ErrOffsetMismatch)) {
				// The chunk may have been accepted in full before the response
				// was lost, or only partly before the connection dropped. Ask
				// the server how much it has so the chunk can be resumed from
				// there instead of being sent again from the start.
				if off, herr := u.serverOffset(ctx, options...); herr == nil {
					switch {
					case off == wantOffset:
						newOffset = off
						curOffset = off
						return false, nil
					case off > curOffset && off < wantOffset:
						curOffset = off
						progressed = true
						return true, err
					case errors.Is(err, ErrOffsetMismatch) && off >= offset && off < wantOffset && off != curOffset:
						curOffset = off
						return true, err
					}
				}
			}
			return retry, err
		})
		if err != nil && !progressed {
			// FIXME What do we do if the upload is still incomplete? Remove
			// the entire upload?
			// See https://github.com/tus/tusd/issues/176
			//
			// The pacer has already used the low level retry budget for this
			// chunk, so stop the outer low level retry restarting the whole
			// upload from its first chunk. Keep context errors intact as the
			// caller needs to see them.
			if ctx.Err() != nil {
				return 0, fmt.Errorf("uploadChunk failed: %w", err)
			}
			return 0, fmt.Errorf("uploadChunk failed: %w", fserrors.NoLowLevelRetryError(errors.New(err.Error())))
		}
	}

	return newOffset, nil
}

// serverOffset queries the server via a tus HEAD request for the
// number of bytes accepted for this upload so far.
func (u *Uploader) serverOffset(ctx context.Context, options ...fs.OpenOption) (int64, error) {
	opts := rest.Opts{
		Method:       "HEAD",
		RootURL:      u.url,
		NoResponse:   true,
		ExtraHeaders: map[string]string{"Tus-Resumable": "1.0.0"},
		Options:      options,
	}

	// This is called from the pacer callback in uploadChunk, so don't
	// pace or retry it here as that would multiply the retry budget and
	// reset the pacer's backoff.
	res, err := u.fs.srv.Call(ctx, &opts)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
}

// Upload uploads the entire body to the server.
func (u *Uploader) Upload(ctx context.Context, options ...fs.OpenOption) error {
	cnt := 1

	fs.Debug(u.fs, "Uploaded starts")
	for u.offset < u.upload.size {
		err := u.UploadChunk(ctx, cnt, options...)
		cnt++
		if err != nil {
			return err
		}
	}
	fs.Debug(u.fs, "-- Uploaded finished")

	return nil
}

// UploadChunk uploads a single chunk.
func (u *Uploader) UploadChunk(ctx context.Context, cnt int, options ...fs.OpenOption) error {
	want := min(int64(u.fs.opt.ChunkSize), u.upload.size-u.offset)
	if want <= 0 {
		return ErrChunkSize
	}
	if int64(cap(u.buf)) < want {
		u.buf = make([]byte, want)
	}
	data := u.buf[:want]

	// Read the chunk sequentially. ReadFull retries short reads, and
	// data is sized to the exact number of bytes expected for this
	// chunk, so any EOF or ErrUnexpectedEOF means the stream ended
	// before u.upload.size bytes were read.
	size, err := io.ReadFull(u.upload.stream, data)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			fs.Errorf(u.fs, "Chunk %d: Error: the stream ended after %d of %d bytes", cnt, u.offset+int64(size), u.upload.size)
			return io.ErrUnexpectedEOF
		}
		fs.Errorf(u.fs, "Chunk %d: Error: Can not read from data stream: %v", cnt, err)
		return err
	}

	wantOffset := u.offset + int64(size)
	newOffset, err := u.uploadChunk(ctx, data[:size], u.offset, options...)
	if err != nil {
		fs.Errorf(u.fs, "Uploaded chunk no %d failed: %v", cnt, err)
		return err
	}

	if newOffset != wantOffset {
		fs.Errorf(u.fs, "Uploaded chunk no %d offset mismatch: server reported %d, expected %d", cnt, newOffset, wantOffset)
		return ErrOffsetMismatch
	}

	fs.Debugf(u.fs, "Uploaded chunk no %d ok, range %d -> %d", cnt, u.offset, newOffset)

	u.offset = newOffset

	return nil
}

// NewUploader creates a new Uploader.
func NewUploader(f *Fs, url string, upload *Upload) *Uploader {
	return &Uploader{
		fs:     f,
		url:    url,
		upload: upload,
	}
}
