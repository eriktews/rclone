package webdav

// Tests for the tus upload path used by the "infinitescale" vendor
// (ownCloud Infinite Scale / OpenCloud).
//
// The Uploader broadcasts progress notifications from a background
// goroutine. That goroutine waits on a channel which must be closed when
// the upload is finished, otherwise it blocks forever, pinning the
// Uploader and the upload buffers in memory for every file uploaded.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

// nonSeekable wraps a reader to hide io.ReadSeeker, matching the reader
// the copy pipeline passes to Update: fs/operations wraps the source
// stream in an accounting reader which does not implement io.Seeker.
type nonSeekable struct {
	r io.Reader
}

func (n *nonSeekable) Read(p []byte) (int, error) { return n.r.Read(p) }

// fakeTusServer implements the small part of the tus protocol that the
// webdav backend uses: POST to create an upload, PATCH to append chunks
// and HEAD to ask for the offset of an upload. It records the bytes
// received per upload so tests can check them.
type fakeTusServer struct {
	srv *httptest.Server
	mu  sync.Mutex
	// retainData controls whether received data is kept for inspection.
	retainData bool
	data       map[string][]byte
	total      map[string]int64
	nextID     int
	// When blockRelease is set, the first PATCH of each upload blocks
	// until it is closed, and patchArrived is closed when that happens.
	blockRelease chan struct{}
	patchArrived chan struct{}
	arrivedOnce  sync.Once
	// dropResponseBeforeAccept is the number of incoming PATCH requests
	// whose connection is closed before accepting the body.
	dropResponseBeforeAccept int
	// acceptPrefix is the number of bytes of the body which the server
	// accepts of the first PATCH of an upload. The rest of the body is
	// discarded and the new offset is reported, which is what a server
	// may do when it does not accept a whole chunk.
	acceptPrefix int
	// redirectFirstPatch makes the server answer the first PATCH with a
	// 307 redirect to the same URL.
	redirectFirstPatch bool
	// redirected records whether the first PATCH was redirected.
	redirected bool
	// patches is the number of PATCH requests received.
	patches int
}

func newFakeTusServer() *fakeTusServer {
	f := &fakeTusServer{
		retainData: true,
		data:       make(map[string][]byte),
		total:      make(map[string]int64),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			f.handlePost(w, r)
		case "PATCH":
			f.handlePatch(w, r)
		case "HEAD":
			f.handleHead(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *fakeTusServer) handlePost(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	w.Header().Set("Location", fmt.Sprintf("http://%s/tus/upload-%d", r.Host, f.nextID))
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeTusServer) handleHead(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	off := f.total[r.URL.Path]
	f.mu.Unlock()
	w.Header().Set("Upload-Offset", strconv.FormatInt(off, 10))
	w.WriteHeader(http.StatusOK)
}

// dropConnection closes the connection of the request without answering
// it, which is what a server does when it loses the response.
func dropConnection(w http.ResponseWriter) {
	if hj, ok := w.(http.Hijacker); ok {
		if conn, _, err := hj.Hijack(); err == nil {
			_ = conn.Close()
		}
	}
}

func (f *fakeTusServer) handlePatch(w http.ResponseWriter, r *http.Request) {
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	requestedOffset, err := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// The handler must not hold the mutex while it blocks.
	f.mu.Lock()
	f.patches++
	if f.redirectFirstPatch && !f.redirected {
		f.redirected = true
		f.mu.Unlock()
		// Answer with a redirect so the client has to send the body again.
		w.Header().Set("Location", r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}
	if f.dropResponseBeforeAccept > 0 {
		f.dropResponseBeforeAccept--
		f.mu.Unlock()
		dropConnection(w)
		return
	}
	if requestedOffset != f.total[r.URL.Path] {
		f.mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		return
	}
	first := f.total[r.URL.Path] == 0
	accept := buf
	if first && f.acceptPrefix > 0 && len(accept) > f.acceptPrefix {
		accept = accept[:f.acceptPrefix]
	}
	f.total[r.URL.Path] += int64(len(accept))
	if f.retainData {
		f.data[r.URL.Path] = append(f.data[r.URL.Path], accept...)
	}
	newOffset := f.total[r.URL.Path]
	f.mu.Unlock()

	if first && f.blockRelease != nil {
		f.arrivedOnce.Do(func() { close(f.patchArrived) })
		<-f.blockRelease
	}

	w.Header().Set("Upload-Offset", strconv.FormatInt(newOffset, 10))
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeTusServer) received(path string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[path]
}

// patchCount returns the number of PATCH requests received.
func (f *fakeTusServer) patchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.patches
}

// release drops the data kept by the server so it does not count
// towards the heap measurements in the tests.
func (f *fakeTusServer) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = nil
}

func (f *fakeTusServer) Close() { f.srv.Close() }

// newTestTusFs makes an Fs configured like the "infinitescale" vendor,
// uploading to a local httptest server.
func newTestTusFs(srvURL string) *Fs {
	ctx := context.Background()
	f := &Fs{
		name: "testtus",
		opt:  Options{Vendor: "infinitescale", ChunkSize: 1024 * 1024}, // 1 MiB chunks
		srv:  rest.NewClient(fshttp.NewClient(ctx)),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(
			pacer.MinSleep(10*time.Millisecond),
			pacer.MaxSleep(100*time.Millisecond),
		)),
	}
	f.endpointURL = srvURL
	f.canTus = true
	return f
}

func newTestObject(f *Fs, size int64) (*Object, fs.ObjectInfo) {
	modTime := time.Now()
	o := &Object{
		fs:      f,
		remote:  "test.bin",
		size:    size,
		modTime: modTime,
	}
	src := object.NewStaticObjectInfo("test.bin", modTime, size, false, nil, nil)
	return o, src
}

// uploadViaTus uploads one file through the real updateViaTus path.
func uploadViaTus(t *testing.T, f *Fs, data []byte) error {
	t.Helper()
	o, src := newTestObject(f, int64(len(data)))
	return o.updateViaTus(context.Background(), &nonSeekable{bytes.NewReader(data)}, "application/octet-stream", src)
}

// uploadViaTusReader uploads size bytes through the real updateViaTus
// path, reading them from r.
func uploadViaTusReader(t *testing.T, f *Fs, size int64, r io.Reader) error {
	t.Helper()
	o, src := newTestObject(f, size)
	return o.updateViaTus(context.Background(), r, "application/octet-stream", src)
}

// countGoroutines returns the number of goroutines whose stack contains
// the needle. The goroutine pprof profile aggregates goroutines with
// identical stacks into a single entry with a "N @" count prefix, so sum
// the counts of the matching entries.
func countGoroutines(t *testing.T, needle string) int {
	t.Helper()
	buf := new(bytes.Buffer)
	if err := pprof.Lookup("goroutine").WriteTo(buf, 1); err != nil {
		t.Fatalf("failed to write goroutine profile: %v", err)
	}
	text := buf.String()
	// Strip the "goroutine profile: total N" header line which shares the
	// first entry's paragraph.
	if head, rest, ok := strings.Cut(text, "\n"); ok && strings.HasPrefix(head, "goroutine profile:") {
		text = rest
	}
	total := 0
	for _, entry := range strings.Split(text, "\n\n") {
		if !strings.Contains(entry, needle) {
			continue
		}
		count, _, ok := strings.Cut(strings.TrimSpace(entry), "@")
		if !ok {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(count)); err == nil {
			total += n
		}
	}
	return total
}

// waitForZeroGoroutines polls until no goroutine matching the needle is
// left, failing the test if they are still running after the timeout.
func waitForZeroGoroutines(t *testing.T, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if n := countGoroutines(t, needle); n == 0 {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("%d goroutine(s) containing %q still running after %v", n, needle, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// uploadHolder holds the data of an upload. A weak pointer to it shows
// whether the data of a finished upload is still reachable.
type uploadHolder struct {
	data []byte
}

// uploadHolderReader reads the data of an uploadHolder, so the upload
// keeps the holder alive as long as it keeps the reader.
type uploadHolderReader struct {
	holder *uploadHolder
}

func (r *uploadHolderReader) Read(p []byte) (int, error) {
	if len(r.holder.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.holder.data)
	r.holder.data = r.holder.data[n:]
	return n, nil
}

// gcAndHeap lets transient goroutines settle, forces two garbage
// collection cycles and returns the size of the live heap afterwards.
func gcAndHeap() uint64 {
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// heapDelta returns the increase in live heap since before, or 0 if
// the heap shrank.
func heapDelta(before uint64) uint64 {
	after := gcAndHeap()
	if after <= before {
		return 0
	}
	return after - before
}

// TestTusUpload uploads a multi-chunk file through the tus path and
// checks the server received the data intact.
func TestTusUpload(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	f := newTestTusFs(srv.srv.URL)

	// 2.5 MiB with 1 MiB chunk size exercises the multi-chunk loop
	data := make([]byte, 25*128*1024)
	for i := range data {
		data[i] = byte(i)
	}
	if err := uploadViaTus(t, f, data); err != nil {
		t.Fatal(err)
	}
	if got := srv.received("/tus/upload-1"); !bytes.Equal(got, data) {
		t.Fatalf("uploaded %d bytes, want %d bytes, content mismatch", len(got), len(data))
	}
}

// TestTusUploaderNoGoroutineLeak checks that uploading several files
// leaves no progress broadcasting goroutines behind.
func TestTusUploaderNoGoroutineLeak(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	f := newTestTusFs(srv.srv.URL)

	const n = 20
	for i := range n {
		data := make([]byte, 512*1024)
		if err := uploadViaTus(t, f, data); err != nil {
			t.Fatalf("upload %d failed: %v", i, err)
		}
	}
	waitForZeroGoroutines(t, "broadcastProgress", 3*time.Second)
}

// TestTusUploaderNoMemoryRetention checks that the memory of uploaded
// files is garbage collected once the uploads are finished. With the
// leak, roughly twice the total size of the uploads stays reachable
// because the blocked progress goroutine pins the buffered file data.
//
// The data of an upload is only reachable through the uploader, so once
// the upload has finished and nothing refers to the uploader any more,
// the data must be collectable. A weak pointer to the data of the upload
// shows whether that is the case.
func TestTusUploaderNoMemoryRetention(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	f := newTestTusFs(srv.srv.URL)

	const (
		n        = 30
		fsize    = 1024 * 1024
		uploaded = n * fsize
		maxKept  = 5 * 1024 * 1024
	)

	// The weak pointer is made in a function so that the only reference
	// to the data of the upload which is left afterwards is the weak
	// pointer itself.
	released := func() weak.Pointer[uploadHolder] {
		holder := &uploadHolder{data: make([]byte, fsize)}
		ref := weak.Make(holder)
		if err := uploadViaTusReader(t, f, int64(fsize), &uploadHolderReader{holder: holder}); err != nil {
			t.Fatalf("upload failed: %v", err)
		}
		return ref
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		if released.Value() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the data of the finished upload is still reachable")
		}
		time.Sleep(10 * time.Millisecond)
	}

	before := gcAndHeap()
	for i := range n {
		data := make([]byte, fsize)
		if err := uploadViaTus(t, f, data); err != nil {
			t.Fatalf("upload %d failed: %v", i, err)
		}
	}
	// The broadcastProgress goroutines must have exited before their
	// references to the upload buffers can be collected, and the fake
	// server must not hold on to the received data either.
	waitForZeroGoroutines(t, "broadcastProgress", 3*time.Second)
	srv.release()
	retained := heapDelta(before)
	t.Logf("uploaded %d files of %d KiB, heap retained after GC: %d KiB", n, fsize/1024, retained/1024)
	if retained > maxKept {
		t.Fatalf("uploaded %d MiB but %d KiB is still retained after GC - more than the %d KiB allowance",
			uploaded/(1024*1024), retained/1024, maxKept/1024)
	}
}

// TestTusUploadCancelledContext checks that cancelling the context
// while a PATCH is in flight stops the upload immediately instead of
// retrying or querying the server offset.
func TestTusUploadCancelledContext(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.blockRelease = make(chan struct{})
	srv.patchArrived = make(chan struct{})
	unblock := sync.OnceFunc(func() { close(srv.blockRelease) })
	defer unblock()

	f := newTestTusFs(srv.srv.URL)
	// Make retries after cancellation slow so this fails if the uploader
	// ignores the cancelled context instead of stopping immediately.
	f.pacer.SetRetries(100)
	f.pacer.SetCalculator(pacer.NewDefault(
		pacer.MinSleep(100*time.Millisecond),
		pacer.MaxSleep(100*time.Millisecond),
	))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		o, src := newTestObject(f, 2048)
		errCh <- o.updateViaTus(ctx, &nonSeekable{bytes.NewReader(make([]byte, 2048))}, "application/octet-stream", src)
	}()

	select {
	case <-srv.patchArrived:
	case err := <-errCh:
		t.Fatalf("upload finished before the first PATCH was blocked: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the first PATCH to arrive")
	}
	cancel()
	unblock()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want %v", err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the upload to stop")
	}
	if got := srv.patchCount(); got != 1 {
		t.Fatalf("server received %d PATCH requests, want 1", got)
	}
}

// TestTusUploadLostResponseNotAccepted checks that when every PATCH
// attempt drops the connection before the server records the chunk, the
// upload fails with the underlying transport error rather than
// ErrOffsetMismatch.
func TestTusUploadLostResponseNotAccepted(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.dropResponseBeforeAccept = 100
	f := newTestTusFs(srv.srv.URL)
	f.pacer.SetRetries(3)

	err := uploadViaTus(t, f, make([]byte, 2048))
	if err == nil {
		t.Fatal("expected upload to fail when connection is always dropped")
	}
	if errors.Is(err, ErrOffsetMismatch) {
		t.Fatalf("got %v, want underlying transport error instead of ErrOffsetMismatch", err)
	}
}

// TestTusUploadPartialChunkAccepted checks that the uploader sends the
// rest of a chunk which the server accepted only in part.
//
// The tus protocol allows the server to accept fewer bytes than were
// offered in a PATCH and to report the new offset in the Upload-Offset
// header of its 204 response. The client has to send the remaining
// bytes starting at that offset, otherwise the file on the server is
// truncated.
//
// The server here stores the first 512 KiB of the first chunk only and
// reports that offset. The uploader has to send the remaining 512 KiB,
// so the server sees two PATCH requests for the first chunk.
func TestTusUploadPartialChunkAccepted(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.acceptPrefix = 512 * 1024
	f := newTestTusFs(srv.srv.URL)

	data := make([]byte, 2*1024*1024)
	for i := range data {
		data[i] = byte(i)
	}
	if err := uploadViaTus(t, f, data); err != nil {
		t.Fatal(err)
	}
	if got := srv.received("/tus/upload-1"); !bytes.Equal(got, data) {
		t.Fatalf("uploaded %d bytes, want %d bytes, content mismatch", len(got), len(data))
	}
	// The first chunk is sent twice, once with all of it and once with
	// the part the server did not accept, the second chunk is sent once.
	if got := srv.patchCount(); got != 3 {
		t.Fatalf("server received %d PATCH requests, want 3", got)
	}
}

// TestTusUploadReplaysBodyOnRedirect checks that the body of a chunk can
// be sent again when the server redirects the request.
//
// The HTTP client of Go can only repeat the body of a request if the
// request has a GetBody function, otherwise it refuses to follow a 307
// or a 308 redirect, and the same is true when an HTTP/2 server asks
// for the request to be repeated on another connection. The uploader has
// to provide GetBody for the chunk it sends.
//
// The server here answers the first PATCH with a 307 to the same URL.
// The upload has to complete and the data has to arrive intact.
func TestTusUploadReplaysBodyOnRedirect(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.redirectFirstPatch = true
	f := newTestTusFs(srv.srv.URL)

	data := make([]byte, 2*1024*1024)
	for i := range data {
		data[i] = byte(i)
	}
	if err := uploadViaTus(t, f, data); err != nil {
		t.Fatal(err)
	}
	if got := srv.received("/tus/upload-1"); !bytes.Equal(got, data) {
		t.Fatalf("uploaded %d bytes, want %d bytes, content mismatch", len(got), len(data))
	}
}
