package webdav

// Tests for the tus upload path used by the "infinitescale" vendor
// (ownCloud Infinite Scale / OpenCloud).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"
	"weak"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
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
// webdav backend uses: POST to create an upload, PATCH to append chunks.
// It records the bytes received per upload so tests can check them.
type fakeTusServer struct {
	srv *httptest.Server
	mu  sync.Mutex
	// retainData controls whether received data is kept for inspection.
	// Large uploads should disable it so the server does not hold the
	// uploaded file in memory itself.
	retainData bool
	data       map[string][]byte
	total      map[string]int64
	crc        map[string]uint32
	created    []string // locations handed out by POST, in order
	nextID     int
	// When blockRelease is set, the first PATCH of each upload blocks
	// until it is closed, and patchArrived is closed when that happens.
	blockRelease chan struct{}
	patchArrived chan struct{}
	arrivedOnce  sync.Once
	// failPatches is the number of incoming PATCH requests to reject
	// with 503 Service Unavailable after reading their body.
	failPatches int
	// dropResponseAfterAccept is the number of incoming PATCH requests
	// whose connection is closed after accepting the body, simulating a
	// lost HTTP response.
	dropResponseAfterAccept int
	// dropResponseBeforeAccept is the number of incoming PATCH requests
	// whose connection is closed before accepting the body.
	dropResponseBeforeAccept int
	// partialPatch is the number of bytes of the first PATCH of an
	// upload to accept before dropping the connection, or zero to accept
	// whole chunks.
	partialPatch int
	// patches is the number of PATCH requests received.
	patches int
	// relativeLocation makes POST return a path-only Location header.
	relativeLocation bool
	// omitLocation makes POST omit the Location header on 201 responses.
	omitLocation bool
	// locationPath sets the path of the Location header, defaulting to
	// /tus/upload-N.
	locationPath string
	// locationQuery is added to the Location header as a query string.
	locationQuery string
	// foreignLocation makes POST return a Location header on a host
	// which is not the host of the test server.
	foreignLocation bool
	// dropHeadResponse makes the HEAD handler drop the connection
	// instead of answering, so the offset query fails as well.
	dropHeadResponse bool
	// failStatus, if not zero, is the status code answered for the
	// first failStatusCount PATCH requests. The body of those requests
	// is stored as usual, so the server behaves like a proxy which lost
	// the response of a request it had already passed on.
	failStatus      int
	failStatusCount int
	// acceptPrefix is the number of bytes of the body which the server
	// accepts of the first PATCH of an upload. The rest of the body is
	// discarded and the new offset is reported, which is what a server
	// may do when it does not accept a whole chunk.
	acceptPrefix int
	// offsetOverride, if not zero, is reported as the Upload-Offset of
	// the PATCH responses instead of the real offset.
	offsetOverride int64
	// redirectFirstPatch makes the server answer the first PATCH with a
	// 307 redirect to the same URL.
	redirectFirstPatch bool
	// redirected records whether the first PATCH was redirected.
	redirected bool
	// posts counts the POST requests which create an upload.
	posts int
	// heads counts the HEAD requests which query the upload offset.
	heads int
	// propfinds counts the PROPFIND requests.
	propfinds int
	// patchURI is the request URI of the last PATCH request.
	patchURI string
	// postHeader is a header of the last POST request.
	postHeader http.Header
}

func newFakeTusServer() *fakeTusServer {
	f := &fakeTusServer{
		retainData: true,
		data:       make(map[string][]byte),
		total:      make(map[string]int64),
		crc:        make(map[string]uint32),
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
		case "MKCOL":
			// The upload creates the parent directory first.
			w.WriteHeader(http.StatusCreated)
		case "PROPFIND":
			f.mu.Lock()
			f.propfinds++
			f.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
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
	f.posts++
	f.postHeader = r.Header.Clone()
	if f.omitLocation {
		w.WriteHeader(http.StatusCreated)
		return
	}
	f.nextID++
	location := fmt.Sprintf("http://%s/tus/upload-%d", r.Host, f.nextID)
	if f.foreignLocation {
		location = "http://foreign.invalid/tus/upload-1"
	}
	if f.locationPath != "" {
		location = fmt.Sprintf("http://%s%s", r.Host, f.locationPath)
	}
	if f.relativeLocation {
		location = fmt.Sprintf("/tus/upload-%d", f.nextID)
	}
	if f.locationQuery != "" {
		location += "?" + f.locationQuery
	}
	f.created = append(f.created, location)
	w.Header().Set("Location", location)
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeTusServer) handleHead(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.heads++
	off := f.total[r.URL.Path]
	f.mu.Unlock()
	if f.dropHeadResponse {
		dropConnection(w)
		return
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(off, 10))
	w.WriteHeader(http.StatusOK)
}

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
	f.patchURI = r.RequestURI
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
	if f.failPatches > 0 {
		f.failPatches--
		f.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if requestedOffset != f.total[r.URL.Path] {
		f.mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		return
	}
	first := f.total[r.URL.Path] == 0
	accept := buf
	partial := first && f.partialPatch > 0 && len(accept) > f.partialPatch
	if partial {
		accept = accept[:f.partialPatch]
	}
	if first && f.acceptPrefix > 0 && len(accept) > f.acceptPrefix {
		accept = accept[:f.acceptPrefix]
	}
	f.total[r.URL.Path] += int64(len(accept))
	f.crc[r.URL.Path] = crc32.Update(f.crc[r.URL.Path], crc32.IEEETable, accept)
	if f.retainData {
		f.data[r.URL.Path] = append(f.data[r.URL.Path], accept...)
	}
	newOffset := f.total[r.URL.Path]
	dropAfter := false
	if f.dropResponseAfterAccept > 0 {
		f.dropResponseAfterAccept--
		dropAfter = true
	}
	failStatus := 0
	if f.failStatusCount > 0 {
		f.failStatusCount--
		failStatus = f.failStatus
	}
	f.mu.Unlock()

	if dropAfter || partial {
		dropConnection(w)
		return
	}

	if failStatus != 0 {
		// The data is stored but the client is told the request failed.
		w.WriteHeader(failStatus)
		return
	}

	if first && f.blockRelease != nil {
		f.arrivedOnce.Do(func() { close(f.patchArrived) })
		<-f.blockRelease
	}

	offset := newOffset
	if f.offsetOverride > 0 {
		offset = f.offsetOverride
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(offset, 10))
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeTusServer) received(path string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[path]
}

// stats returns the total number of bytes received for the upload at
// path and the CRC32 checksum calculated over them.
func (f *fakeTusServer) stats(path string) (int64, uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.total[path], f.crc[path]
}

// patchCount returns the number of PATCH requests received.
func (f *fakeTusServer) patchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.patches
}

// postCount returns the number of POST requests received, which is the
// number of uploads created on the server.
func (f *fakeTusServer) postCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.posts
}

// headCount returns the number of HEAD requests received, which is the
// number of times the uploader asked the server for its offset.
func (f *fakeTusServer) headCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heads
}

// propfindCount returns the number of PROPFIND requests received.
func (f *fakeTusServer) propfindCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.propfinds
}

// lastPatchURI returns the request URI of the last PATCH request.
func (f *fakeTusServer) lastPatchURI() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.patchURI
}

// postHeaderValue returns the value of a header of the last POST request.
func (f *fakeTusServer) postHeaderValue(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.postHeader.Get(name)
}

// lastCreated returns the upload location last handed out by a POST
// request.
func (f *fakeTusServer) lastCreated() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created[len(f.created)-1]
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
	return newTestTusFsWithClient(srvURL, fshttp.NewClient(context.Background()))
}

// newTestTusFsWithClient makes an Fs like newTestTusFs which uses the
// given HTTP client.
func newTestTusFsWithClient(srvURL string, client *http.Client) *Fs {
	ctx := context.Background()
	f := &Fs{
		name: "testtus",
		opt:  Options{Vendor: "infinitescale", ChunkSize: 1024 * 1024}, // 1 MiB chunks
		srv:  rest.NewClient(client),
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
	return uploadViaTusReader(t, f, int64(len(data)), &nonSeekable{bytes.NewReader(data)})
}

// uploadViaTusReader uploads size bytes through the real updateViaTus
// path, reading them from r.
func uploadViaTusReader(t *testing.T, f *Fs, size int64, r io.Reader) error {
	t.Helper()
	o, src := newTestObject(f, size)
	return o.updateViaTus(context.Background(), r, "application/octet-stream", src)
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

// TestTusUploaderNoGoroutineLeak checks that uploading several files
// leaves no goroutines behind.
//
// The uploader used to start a goroutine per upload which waited for
// progress notifications. Nothing ever read those notifications, so the
// goroutine blocked forever and kept the uploader, and with it the data
// of the upload, alive. Counting the goroutines which are parked in the
// broadcasting function is precise, unlike counting all the goroutines
// of the process, which also counts the ones of the HTTP client and of
// the pacer.
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

// TestTusUploaderNoMemoryRetention checks that the memory of uploaded
// files is garbage collected once the uploads are finished.
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
	srv.release()
	retained := heapDelta(before)
	t.Logf("uploaded %d files of %d KiB, heap retained after GC: %d KiB", n, fsize/1024, retained/1024)
	if retained > maxKept {
		t.Fatalf("uploaded %d MiB but %d KiB is still retained after GC - more than the %d KiB allowance",
			uploaded/(1024*1024), retained/1024, maxKept/1024)
	}
}

// patternReader produces a deterministic byte pattern of size bytes
// without holding the whole file in memory.
type patternReader struct {
	remaining int64
	pos       int64
}

func newPatternReader(size int64) *patternReader {
	return &patternReader{remaining: size}
}

func (p *patternReader) Read(b []byte) (int, error) {
	if p.remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(b))
	if n > p.remaining {
		n = p.remaining
	}
	for i := int64(0); i < n; i++ {
		b[i] = byte((p.pos + i) / 251)
	}
	p.pos += n
	p.remaining -= n
	return int(n), nil
}

// patternCRC returns the CRC32 checksum the server should end up with
// when an upload of size pattern bytes completes.
func patternCRC(size int64) uint32 {
	crc := crc32.NewIEEE()
	_, err := io.Copy(crc, newPatternReader(size))
	if err != nil {
		panic(err) // patternReader never returns an error
	}
	return crc.Sum32()
}

// TestTusUpdateStreamsFileWithoutSpooling checks that the file is
// uploaded chunk by chunk instead of being read into memory first.
//
// The tus uploader seeks in the stream of the upload, so the stream has
// to be a io.ReadSeeker. The copy pipeline hands a non-seekable reader
// to Update, so the whole file was read into memory before the first
// byte was sent. That needs as much memory as the file is big and
// makes the upload of a file which is bigger than the memory of the
// machine impossible.
//
// The test uploads a file which is much bigger than the heap allowance
// of the test and samples the live heap while the first chunk is in
// flight. Reading the file into memory first fails the test with the
// size of the file on the heap instead of the size of a chunk.
func TestTusUpdateStreamsFileWithoutSpooling(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.retainData = false // the server must not hold the uploaded file
	srv.blockRelease = make(chan struct{})
	srv.patchArrived = make(chan struct{})
	// Unblock the stalled PATCH on any exit path, otherwise the upload
	// goroutine would keep the connection open and Server.Close() would
	// hang.
	unblockUpload := sync.OnceFunc(func() { close(srv.blockRelease) })
	defer unblockUpload()

	const (
		fsize   = 64 << 20 // 64 MiB
		maxHeap = 8 << 20  // one 1 MiB chunk buffer plus slack
	)
	f := newTestTusFs(srv.srv.URL)
	f.opt.ChunkSize = 1024 * 1024

	wantCRC := patternCRC(fsize)

	heapBefore := gcAndHeap()
	errCh := make(chan error, 1)
	go func() {
		errCh <- uploadViaTusReader(t, f, fsize, newPatternReader(fsize))
	}()

	// Wait for the first PATCH to reach the server and let it block
	// there. If the file is spooled into memory before the upload
	// starts, the live heap is now roughly the size of the whole file.
	select {
	case <-srv.patchArrived:
	case err := <-errCh:
		t.Fatalf("upload finished before the first PATCH was blocked: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the first PATCH to arrive")
	}

	inUse := heapDelta(heapBefore)
	t.Logf("heap used by the upload while the first PATCH is blocked: %d KiB", inUse/1024)
	if inUse > maxHeap {
		t.Fatalf("the upload holds %d KiB on the heap while uploading the first of %d chunks, more than the %d KiB allowance - the file was spooled into memory",
			inUse/1024, fsize/(1024*1024), maxHeap/1024)
	}
	unblockUpload()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the upload to finish")
	}

	location, err := url.Parse(srv.lastCreated())
	if err != nil {
		t.Fatal(err)
	}
	gotTotal, gotCRC := srv.stats(location.Path)
	if gotTotal != fsize {
		t.Fatalf("server received %d bytes, want %d", gotTotal, fsize)
	}
	if gotCRC != wantCRC {
		t.Fatalf("uploaded data is corrupted: CRC32 %08x, want %08x", gotCRC, wantCRC)
	}
}

// TestTusUploadTruncatedStream checks that an upload of a stream which
// ends before the announced size fails with io.ErrUnexpectedEOF.
//
// The size of the upload is announced to the server in the creation
// request, so the server waits for that many bytes. A stream which ends
// early means that the file changed while it was uploaded, and the
// upload has to fail with an error which says so. If the uploader
// treats the end of the stream as an empty chunk instead, the offset of
// the upload never advances and the upload sends empty chunks forever.
func TestTusUploadTruncatedStream(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	f := newTestTusFs(srv.srv.URL)
	f.opt.ChunkSize = 1024

	// Stream ends with EOF mid-chunk before the advertised 2048 bytes.
	err := uploadViaTusReader(t, f, 2048, newPatternReader(500))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got err %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if got, _ := srv.stats("/tus/upload-1"); got != 0 {
		t.Fatalf("truncated chunk should not be sent, server received %d bytes", got)
	}

	// Stream fails with io.ErrUnexpectedEOF at a chunk boundary (0 bytes read).
	r := io.MultiReader(newPatternReader(1024), iotest.ErrReader(io.ErrUnexpectedEOF))
	err = uploadViaTusReader(t, f, 2048, r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got err %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if got, _ := srv.stats("/tus/upload-2"); got != 1024 {
		t.Fatalf("server received %d bytes, want 1024", got)
	}
}

// TestTusUploadZeroChunkSize checks that a chunk size of zero fails
// with an error instead of sending empty chunks forever.
//
// The chunk size is a setting of the client, so a chunk size of zero
// means that the upload cannot make any progress. The uploader has to
// notice that before it creates the upload on the server, otherwise
// every failed upload leaves an upload resource behind.
func TestTusUploadZeroChunkSize(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	f := newTestTusFs(srv.srv.URL)
	f.opt.ChunkSize = 0

	err := uploadViaTus(t, f, make([]byte, 2048))
	if !errors.Is(err, ErrChunkSize) {
		t.Fatalf("got err %v, want %v", err, ErrChunkSize)
	}
	// The chunk size is a setting of the client, so the upload has to be
	// rejected before it is created on the server. Otherwise every
	// failed upload leaves an upload resource behind.
	if got := srv.postCount(); got != 0 {
		t.Fatalf("server got %d POST requests, want 0 - no upload must be created", got)
	}
}

// TestTusUploadRetryChunk checks that a chunk which fails is sent again
// in full.
//
// The pacer retries a failed PATCH request and the body of the request
// has to be sent again with it. A body which is a stream cannot be read
// twice, so the uploader has to keep the chunk in memory until the
// server has accepted it. Otherwise the retry sends an empty body and
// the file on the server is truncated.
func TestTusUploadRetryChunk(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.failPatches = 2
	f := newTestTusFs(srv.srv.URL)

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

// TestTusUploadLostResponseRecovers checks that a chunk which the
// server stored but whose response was lost is not sent again.
//
// The server may have stored the chunk even though the client did not
// see the response, for instance because the connection was dropped on
// the way back. Sending the chunk again then makes the server answer
// "409 Conflict" and the upload fails, so the uploader has to ask the
// server for its offset with a HEAD request and to continue from there.
func TestTusUploadLostResponseRecovers(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.dropResponseAfterAccept = 1
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
	// The first chunk was accepted before its response was lost. The
	// uploader must recover via HEAD without sending it again, so the
	// server sees exactly one PATCH for each of the two chunks.
	if got := srv.patchCount(); got != 2 {
		t.Fatalf("server received %d PATCH requests, want 2", got)
	}
}

// TestTusUploadPartialChunkRecovers checks that a chunk which the
// server stored only in part is completed.
//
// The server stores the bytes it received before the connection
// dropped, so the client has to ask the server for its offset and to
// send the rest of the chunk. Sending the whole chunk again would make
// the server answer "409 Conflict", because it has already stored the
// first part of it.
func TestTusUploadPartialChunkRecovers(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.partialPatch = 512 * 1024
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
	// The first chunk is sent once, dropped after 512 KiB, then resumed
	// with the remaining 512 KiB, followed by the second chunk.
	if got := srv.patchCount(); got != 3 {
		t.Fatalf("server received %d PATCH requests, want 3", got)
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

// TestTusUploadRelativeLocation checks that a relative Location header
// of the creation request is resolved against the URL of the remote.
//
// The tus server may answer the creation request with a path instead of
// a full URL. The uploader has to resolve it, otherwise the request for
// the chunk is sent to a URL without a host and the upload fails.
func TestTusUploadRelativeLocation(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.relativeLocation = true
	f := newTestTusFs(srv.srv.URL)

	data := make([]byte, 2048)
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

// TestTusUploadMissingLocation checks that a creation request which
// does not return a Location header fails with a clear error.
//
// The Location header of the creation request holds the URL of the
// created upload, so without it there is no upload to send the data to.
// The uploader has to say that instead of sending the data to an empty
// URL.
func TestTusUploadMissingLocation(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.omitLocation = true
	f := newTestTusFs(srv.srv.URL)

	err := uploadViaTus(t, f, make([]byte, 2048))
	if err == nil {
		t.Fatal("expected error when server omits Location header")
	}
	if !strings.Contains(err.Error(), "upload location") {
		t.Fatalf("expected clear upload location error, got: %v", err)
	}
}

// TestTusUploadOneHeadProbePerRetry checks that a failed chunk PATCH
// makes the uploader send a single tus HEAD request for the offset.
//
// The uploader asks the server how much of the chunk it has stored when
// a PATCH fails, because the server may have accepted the chunk even
// though the response was lost. That query has to be a single probe: if
// it were retried with the pacer as well, an outage would multiply the
// number of requests per chunk and every successful probe would reset
// the backoff of the pacer.
//
// The server here drops every connection, so the PATCH and the HEAD
// both fail. With a retry budget of 3 the uploader has to send 3 PATCH
// requests and at most 3 HEAD requests.
func TestTusUploadOneHeadProbePerRetry(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.dropResponseBeforeAccept = 1000 // every PATCH is dropped
	srv.dropHeadResponse = true         // and so is every HEAD
	f := newTestTusFs(srv.srv.URL)
	const retries = 3
	f.pacer.SetRetries(retries)

	if err := uploadViaTus(t, f, make([]byte, 2048)); err == nil {
		t.Fatal("expected the upload to fail")
	}
	if got := srv.patchCount(); got != retries {
		t.Errorf("got %d PATCH requests, want %d", got, retries)
	}
	if got := srv.headCount(); got == 0 {
		t.Error("got no HEAD requests, want at least one - the offset of the server has to be checked")
	} else if got > retries {
		t.Errorf("got %d HEAD requests, want at most %d - the offset query must not be retried", got, retries)
	}
}

// TestTusUploadRecoversFromBadGateway checks that a chunk which the
// server stored but whose response was lost is not sent again.
//
// A reverse proxy in front of the tus server can answer a PATCH with
// "502 Bad Gateway" after the tus server has already stored the chunk.
// The client cannot tell that apart from a real failure, so it has to
// ask the server for its offset before sending the chunk again. Sending
// it again blindly makes the server answer "409 Conflict" and the
// upload fails.
//
// The server here stores the body of the first PATCH but answers it
// with 502. The upload has to complete and the chunk must not be sent
// twice.
func TestTusUploadRecoversFromBadGateway(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.failStatus = http.StatusBadGateway
	srv.failStatusCount = 1
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
	// The first chunk was stored, so the uploader has to ask for the
	// offset instead of sending the chunk again: one PATCH for each of
	// the two chunks.
	if got := srv.patchCount(); got != 2 {
		t.Fatalf("server received %d PATCH requests, want 2", got)
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

// TestTusUploadRejectsBogusOffset checks that an offset beyond the end
// of the chunk is rejected.
//
// The tus offset is the number of bytes the server has stored, so it can
// never be larger than the number of bytes which were sent. Accepting
// such an offset would make rclone believe that the upload is further
// along than it is, so the rest of the file would be skipped and the
// file on the server would be silently truncated.
//
// The server here answers every PATCH with an offset of 1000000. The
// upload has to fail.
func TestTusUploadRejectsBogusOffset(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.offsetOverride = 1000000
	f := newTestTusFs(srv.srv.URL)

	err := uploadViaTus(t, f, make([]byte, 2048))
	if err == nil {
		t.Fatal("expected the upload to fail")
	}
	if !strings.Contains(err.Error(), ErrOffsetMismatch.Error()) {
		t.Fatalf("expected an offset mismatch, got: %v", err)
	}
}

// TestTusUploadNoRestartAfterChunkRetries checks that a chunk which
// cannot be uploaded does not make rclone upload the whole file again.
//
// The uploader retries a failed chunk with the pacer, so the low level
// retry budget of the chunk is used up before the error reaches the
// caller. The error must therefore not ask the copy loop to retry the
// file, which would send all the chunks the server has already accepted
// again.
//
// The server here fails every PATCH with 503. The copy has to fail and
// only one upload may be created on the server.
func TestTusUploadNoRestartAfterChunkRetries(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.failPatches = 1000

	configfile.Install()
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 3

	// The destination is a real webdav remote, so the copy goes through
	// the same code as a copy of a file to a remote.
	f, err := NewFs(ctx, "testtus", "", configmap.Simple{
		"type":   "webdav",
		"url":    srv.srv.URL,
		"vendor": "infinitescale",
	})
	if err != nil {
		t.Fatal(err)
	}

	// The source is a local file.
	dir := t.TempDir()
	srcFs, err := local.NewFs(ctx, "local", dir, configmap.Simple{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "source.bin"), make([]byte, 1024), 0600); err != nil {
		t.Fatal(err)
	}
	src, err := srcFs.NewObject(ctx, "source.bin")
	if err != nil {
		t.Fatal(err)
	}

	if _, err = operations.Copy(ctx, f, nil, "target.bin", src); err == nil {
		t.Fatal("expected the copy to fail")
	}
	if got := srv.postCount(); got != 1 {
		t.Fatalf("server got %d uploads created, want 1 - the file must not be uploaded again", got)
	}
}

// TestTusUploadKeepsLocationQueryAndPath checks that the upload URL
// which the server returns in the Location header is used unchanged.
//
// The tus server returns the URL of the created upload in the Location
// header of the creation request. That URL can carry a query string,
// for instance a token, and it can contain percent-encoded characters.
// The client has to use the URL as it is: dropping the query makes the
// server reject the upload and unescaping the path changes which
// resource is addressed.
//
// The server here returns a relative Location with a token and an
// escaped slash. The PATCH has to be sent to exactly that URL.
func TestTusUploadKeepsLocationQueryAndPath(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.locationPath = "/tus/upload%2F1"
	srv.locationQuery = "token=abc"
	f := newTestTusFs(srv.srv.URL)

	data := make([]byte, 2048)
	for i := range data {
		data[i] = byte(i)
	}
	if err := uploadViaTus(t, f, data); err != nil {
		t.Fatal(err)
	}
	if got, want := srv.lastPatchURI(), "/tus/upload%2F1?token=abc"; got != want {
		t.Fatalf("got PATCH to %q, want %q", got, want)
	}
}

// TestTusUploadRejectsForeignLocation checks that the uploader refuses
// to send the data of the file to another host.
//
// The Location header of the creation request decides where the data of
// the file is sent, and that request carries the credentials of the
// remote, so a server which is misconfigured or compromised could
// collect them by pointing the upload at a host of its own. The uploader
// must only use a Location on the host of the remote.
//
// The server here returns a Location on another host. The upload has to
// fail without sending any data.
func TestTusUploadRejectsForeignLocation(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	srv.foreignLocation = true
	f := newTestTusFs(srv.srv.URL)

	err := uploadViaTus(t, f, make([]byte, 2048))
	if err == nil {
		t.Fatal("expected the upload to fail")
	}
	if !strings.Contains(err.Error(), "upload location") {
		t.Fatalf("expected an error about the upload location, got: %v", err)
	}
	if got := srv.patchCount(); got != 0 {
		t.Fatalf("server got %d PATCH requests, want 0 - no data must be sent", got)
	}
}

// TestTusUploadRejectsHTTPSDowngrade checks that the uploader refuses an
// upload location which downgrades the connection to plain HTTP.
//
// The credentials of the remote are sent with the upload, so an upload
// location which downgrades an https endpoint to http would send them in
// cleartext.
//
// The server here is an https server which returns a Location with the
// http scheme. The upload has to fail.
func TestTusUploadRejectsHTTPSDowngrade(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://"+r.Host+"/tus/upload-1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	f := newTestTusFsWithClient(srv.URL, srv.Client())

	err := uploadViaTus(t, f, make([]byte, 2048))
	if err == nil {
		t.Fatal("expected the upload to fail")
	}
	// The error is rest.ErrHTTPSDowngrade.
	if !strings.Contains(err.Error(), "HTTPS to HTTP") {
		t.Fatalf("expected an error about the HTTPS downgrade, got: %v", err)
	}
}

// TestTusUploadUsesSourceMetadata checks that the upload is created with
// the metadata of the source file.
//
// The creation request has to carry the modification time of the file
// which is uploaded, and its checksum if the remote supports it. The
// metadata of the destination object is not the same thing: the
// destination usually does not exist yet, so reading it would ask the
// server about a file which is not there and would send the
// modification time of the old file or of the current time.
//
// The server here records the headers of the creation request and counts
// the PROPFIND requests. The request has to carry the modification time
// of the source and no PROPFIND may be sent.
func TestTusUploadUsesSourceMetadata(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	f := newTestTusFs(srv.srv.URL)
	// The infinitescale vendor sends the modification time with the
	// upload.
	f.useOCMtime = true
	modTime := time.Now().Add(-time.Hour).Truncate(time.Second)
	size := int64(2048)
	src := object.NewStaticObjectInfo("test.bin", modTime, size, false, nil, nil)
	o := &Object{fs: f, remote: "test.bin", size: size, modTime: modTime}

	err := o.updateViaTus(context.Background(), &nonSeekable{bytes.NewReader(make([]byte, size))}, "application/octet-stream", src)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := srv.postHeaderValue("X-OC-Mtime"), strconv.FormatInt(modTime.Unix(), 10); got != want {
		t.Fatalf("got X-OC-Mtime %q, want %q", got, want)
	}
	if got := srv.propfindCount(); got != 0 {
		t.Fatalf("server got %d PROPFIND requests, want 0 - the destination must not be read", got)
	}
}

// TestTusUploadRejectsUnknownSize checks that a source of unknown size
// is rejected with a clear error.
//
// The creation request has to announce the length of the upload, and
// rclone needs the size to know where the chunks of the upload end. A
// source of unknown size can therefore not be uploaded with the tus
// protocol. Failing with a clear error is better than announcing
// "Upload-Length: -1" and then uploading nothing, which looks like a
// successful upload of an empty file.
//
// The server here must not see an upload at all.
func TestTusUploadRejectsUnknownSize(t *testing.T) {
	srv := newFakeTusServer()
	defer srv.Close()
	f := newTestTusFs(srv.srv.URL)
	o, src := newTestObject(f, -1)

	err := o.updateViaTus(context.Background(), &nonSeekable{bytes.NewReader(nil)}, "application/octet-stream", src)
	if err == nil {
		t.Fatal("expected the upload to fail")
	}
	if !strings.Contains(err.Error(), "unknown size") {
		t.Fatalf("expected an error about the unknown size, got: %v", err)
	}
	if got := srv.postCount(); got != 0 {
		t.Fatalf("server got %d POST requests, want 0 - no upload must be created", got)
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
