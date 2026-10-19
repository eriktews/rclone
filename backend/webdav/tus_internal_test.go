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
// webdav backend uses: POST to create an upload, PATCH to append chunks.
// It records the bytes received per upload so tests can check them.
type fakeTusServer struct {
	srv    *httptest.Server
	mu     sync.Mutex
	data   map[string][]byte
	nextID int
}

func newFakeTusServer() *fakeTusServer {
	f := &fakeTusServer{data: make(map[string][]byte)}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case "POST":
			f.nextID++
			w.Header().Set("Location", fmt.Sprintf("http://%s/tus/upload-%d", r.Host, f.nextID))
			w.WriteHeader(http.StatusCreated)
		case "PATCH":
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			f.data[r.URL.Path] = append(f.data[r.URL.Path], buf...)
			w.Header().Set("Upload-Offset", strconv.Itoa(len(f.data[r.URL.Path])))
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *fakeTusServer) received(path string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[path]
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
