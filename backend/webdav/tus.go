package webdav

/*
   Chunked upload based on the tus protocol for ownCloud Infinite Scale
   See https://tus.io/protocols/resumable-upload
*/

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

func (o *Object) updateViaTus(ctx context.Context, in io.Reader, contentType string, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {

	fn := filepath.Base(src.Remote())
	metadata := map[string]string{
		"filename": fn,
		"mtime":    strconv.FormatInt(src.ModTime(ctx).Unix(), 10),
		"filetype": contentType,
	}

	// create an upload from a file.
	upload := NewUpload(in, src.Size(), metadata)

	// create the uploader.
	uploader, err := o.CreateUploader(ctx, src, upload, options...)
	if err == nil {
		// start the uploading process.
		err = uploader.Upload(ctx, options...)
	}

	return err
}

func (f *Fs) getTusLocationOrRetry(ctx context.Context, resp *http.Response, err error) (bool, string, error) {

	// resp is nil if the HTTP transaction failed before a response
	// was received, eg on connection refused or reset
	if resp != nil {
		switch resp.StatusCode {
		case 201:
			location := resp.Header.Get("Location")
			return false, location, nil
		case 412:
			return false, "", ErrVersionMismatch
		case 413:
			return false, "", ErrLargeUpload
		}
	}

	retry, err := f.shouldRetry(ctx, resp, err)
	return retry, "", err
}

// CreateUploader creates a new upload to the server.
//
// The upload is created with the metadata of src so the creation
// request does not need to read the destination object.
func (o *Object) CreateUploader(ctx context.Context, src fs.ObjectInfo, u *Upload, options ...fs.OpenOption) (*Uploader, error) {
	if u == nil {
		return nil, ErrNilUpload
	}
	if u.size < 0 {
		return nil, errors.New("tus upload: can't upload a stream of unknown size")
	}
	if u.size > 0 && o.fs.opt.ChunkSize <= 0 {
		return nil, ErrChunkSize
	}

	l := int64(0)
	p := o.filePath()
	// cut the filename off
	dir, _ := filepath.Split(p)
	if dir == "" {
		dir = "/"
	}

	opts := rest.Opts{
		Method:        "POST",
		Path:          dir,
		NoResponse:    true,
		RootURL:       o.fs.endpointURL,
		ContentLength: &l,
		ExtraHeaders:  o.extraHeaders(ctx, src),
		Options:       options,
	}
	opts.ExtraHeaders["Upload-Length"] = strconv.FormatInt(u.size, 10)
	opts.ExtraHeaders["Upload-Metadata"] = u.EncodedMetadata()
	opts.ExtraHeaders["Tus-Resumable"] = "1.0.0"
	// opts.ExtraHeaders["mtime"] = strconv.FormatInt(src.ModTime(ctx).Unix(), 10)

	var tusLocation string
	// rclone http call
	err := o.fs.pacer.Call(func() (bool, error) {
		var retry bool
		res, err := o.fs.srv.Call(ctx, &opts)
		retry, tusLocation, err = o.fs.getTusLocationOrRetry(ctx, res, err)
		return retry, err
	})
	if err != nil {
		return nil, fmt.Errorf("making upload directory failed: %w", err)
	}
	if tusLocation == "" {
		return nil, errors.New("tus upload: server did not return an upload location")
	}
	loc, err := url.Parse(tusLocation)
	if err != nil {
		return nil, fmt.Errorf("tus upload: invalid upload location %q: %w", tusLocation, err)
	}
	if loc.Opaque != "" || (loc.Scheme != "" && loc.Host == "") {
		return nil, fmt.Errorf("tus upload: invalid upload location %q", tusLocation)
	}
	base, err := url.Parse(o.fs.endpointURL)
	if err != nil {
		return nil, fmt.Errorf("tus upload: invalid endpoint URL %q: %w", o.fs.endpointURL, err)
	}
	// Resolve relative locations against the URL the upload was created
	// at, which may be a subdirectory of the endpoint.
	reqURL, err := rest.URLJoin(base, dir)
	if err != nil {
		return nil, fmt.Errorf("tus upload: invalid upload directory %q: %w", dir, err)
	}
	resolved := reqURL.ResolveReference(loc)
	if resolved.Scheme == "" || resolved.Host == "" {
		return nil, fmt.Errorf("tus upload: invalid upload location %q", tusLocation)
	}
	if base.Scheme == "https" && resolved.Scheme != "https" {
		return nil, rest.ErrHTTPSDowngrade
	}
	if !rest.SameHost(base, resolved) {
		return nil, fmt.Errorf("tus upload: upload location %q is not on the endpoint host %q", resolved.Redacted(), base.Redacted())
	}
	tusLocation = resolved.String()

	uploader := NewUploader(o.fs, tusLocation, u)

	return uploader, nil
}
