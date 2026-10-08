package kubedial

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	httpstream "k8s.io/streaming/pkg/httpstream"
)

// fakeStream is one stream a fakeConn handed out.
type fakeStream struct {
	mu      sync.Mutex
	headers http.Header
	resets  int
	readBuf []byte
}

func (s *fakeStream) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.readBuf) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.readBuf)
	s.readBuf = s.readBuf[n:]
	return n, nil
}

func (s *fakeStream) Write(p []byte) (int, error) { return len(p), nil }

func (s *fakeStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

func (s *fakeStream) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resets++
	return nil
}

func (s *fakeStream) Headers() http.Header { return s.headers }
func (s *fakeStream) Identifier() uint32   { return 0 }

func (s *fakeStream) resetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resets
}

// fakeConn is an httpstream.Connection the test drives by hand.
type fakeConn struct {
	mu         sync.Mutex
	failAt     int // 1-based CreateStream call to fail; 0 means never
	errMsg     []byte
	calls      int
	streams    []*fakeStream
	headers    []http.Header
	removed    []httpstream.Stream
	idleSet    bool
	closed     bool
	closeCalls int
	closeErr   error
	closeCh    chan bool
	closeOnce  sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{closeCh: make(chan bool)}
}

func (c *fakeConn) CreateStream(h http.Header) (httpstream.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.headers = append(c.headers, h.Clone())
	if c.failAt != 0 && c.calls == c.failAt {
		return nil, errors.New("fake: create stream failed")
	}
	s := &fakeStream{headers: h.Clone()}
	// The odd-numbered stream of an open is the error stream, which is where the
	// kubelet writes a ForwardError. errMsg makes that readable.
	if c.errMsg != nil && c.calls%2 == 1 {
		s.readBuf = append([]byte(nil), c.errMsg...)
	}
	c.streams = append(c.streams, s)
	return s, nil
}

func (c *fakeConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.closeCalls++
	c.closeOnce.Do(func() { close(c.closeCh) })
	return c.closeErr
}

func (c *fakeConn) CloseChan() <-chan bool { return c.closeCh }

func (c *fakeConn) SetIdleTimeout(time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idleSet = true
}

func (c *fakeConn) RemoveStreams(streams ...httpstream.Stream) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removed = append(c.removed, streams...)
}

func (c *fakeConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeConn) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *fakeConn) headerAt(i int) http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.headers[i]
}

func (c *fakeConn) createdStreams() []*fakeStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*fakeStream(nil), c.streams...)
}

func (c *fakeConn) removedStreams() []httpstream.Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]httpstream.Stream(nil), c.removed...)
}

func (c *fakeConn) idleTimeoutSet() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.idleSet
}

// fakeTransport is both the pool's streamDialerFactory and the dialer it hands
// back, so a test can count both the factory calls and the dials.
type fakeTransport struct {
	mu           sync.Mutex
	factoryCalls int
	dials        int
	conns        []*fakeConn
	queue        []*fakeConn
	newConn      func() *fakeConn
	err          error

	block       chan struct{}
	entered     chan struct{}
	enteredOnce sync.Once
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		newConn: newFakeConn,
		entered: make(chan struct{}),
	}
}

func (f *fakeTransport) factory(_, _ string) (httpstream.Dialer, error) {
	f.mu.Lock()
	f.factoryCalls++
	f.mu.Unlock()
	return &fakeDialer{f: f}, nil
}

func (f *fakeTransport) dialCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials
}

func (f *fakeTransport) factoryCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.factoryCalls
}

func (f *fakeTransport) connAt(i int) *fakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns[i]
}

type fakeDialer struct{ f *fakeTransport }

func (d *fakeDialer) Dial(...string) (httpstream.Connection, string, error) {
	f := d.f
	if f.block != nil {
		f.enteredOnce.Do(func() { close(f.entered) })
		<-f.block
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.dials++
	if f.err != nil {
		return nil, "", f.err
	}
	var conn *fakeConn
	if len(f.queue) > 0 {
		conn = f.queue[0]
		f.queue = f.queue[1:]
	} else {
		conn = f.newConn()
	}
	f.conns = append(f.conns, conn)
	return conn, portforward.PortForwardProtocolV1Name, nil
}

func mustOpen(t *testing.T, p *pool, ep Endpoint) (httpstream.Stream, httpstream.Stream, func()) {
	t.Helper()
	data, errStream, release, err := p.open(t.Context(), ep)
	if err != nil {
		t.Fatalf("open(%s): %v", ep, err)
	}
	return data, errStream, release
}

func closePool(t *testing.T, p *pool) {
	t.Helper()
	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

var testEndpoint = Endpoint{Namespace: "ns", Pod: "pod", Port: 5432}

func TestPoolOneConnectionPerPod(t *testing.T) {
	f := newFakeTransport()
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	_, _, r1 := mustOpen(t, p, testEndpoint)
	_, _, r2 := mustOpen(t, p, testEndpoint)
	r1()
	r2()

	if got := f.dialCount(); got != 1 {
		t.Errorf("dial count = %d, want 1", got)
	}
	if got := f.factoryCount(); got != 1 {
		t.Errorf("factory count = %d, want 1", got)
	}
}

func TestPoolConcurrentOpensDialOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeTransport()
		f.block = make(chan struct{})
		p := newPool(f.factory, time.Minute)
		defer closePool(t, p)

		const n = 20
		var wg sync.WaitGroup
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, release, err := p.open(t.Context(), testEndpoint)
				if err != nil {
					t.Errorf("open: %v", err)
					return
				}
				release()
			}()
		}

		// Wait until every caller is durably blocked: one inside Dial, the rest
		// on that dial's ready channel. Releasing the dial before they arrive
		// would let them find a ready connection and pass a broken singleflight.
		synctest.Wait()
		close(f.block)
		wg.Wait()

		if got := f.dialCount(); got != 1 {
			t.Errorf("dial count = %d, want 1", got)
		}
		if got := f.factoryCount(); got != 1 {
			t.Errorf("factory count = %d, want 1", got)
		}
	})
}

func TestPoolRequestIDsIncrement(t *testing.T) {
	f := newFakeTransport()
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	for range 2 {
		_, _, release := mustOpen(t, p, testEndpoint)
		release()
	}

	conn := f.connAt(0)
	if got := conn.callCount(); got != 4 {
		t.Fatalf("CreateStream calls = %d, want 4", got)
	}
	// Calls are error then data for the first open, then error then data for
	// the second, so the request IDs go 0,0,1,1.
	for i, want := range []string{"0", "0", "1", "1"} {
		if got := conn.headerAt(i).Get(corev1.PortForwardRequestIDHeader); got != want {
			t.Errorf("request id at call %d = %q, want %q", i, got, want)
		}
	}
}

func TestPoolStreamHeaders(t *testing.T) {
	f := newFakeTransport()
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	_, _, release := mustOpen(t, p, testEndpoint)
	release()

	conn := f.connAt(0)
	if got := conn.callCount(); got != 2 {
		t.Fatalf("CreateStream calls = %d, want 2", got)
	}
	want := []map[string]string{
		{corev1.StreamType: corev1.StreamTypeError, corev1.PortHeader: "5432", corev1.PortForwardRequestIDHeader: "0"},
		{corev1.StreamType: corev1.StreamTypeData, corev1.PortHeader: "5432", corev1.PortForwardRequestIDHeader: "0"},
	}
	for i, w := range want {
		h := conn.headerAt(i)
		if len(h) != len(w) {
			t.Errorf("call %d headers = %v, want %v", i, h, w)
			continue
		}
		for k, v := range w {
			if got := h.Get(k); got != v {
				t.Errorf("call %d header %q = %q, want %q", i, k, got, v)
			}
		}
	}
}

func TestPoolDataStreamFailureCleansErrorStream(t *testing.T) {
	conn := newFakeConn()
	conn.failAt = 2 // the data stream of the first open
	f := newFakeTransport()
	f.newConn = func() *fakeConn { return conn }
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	_, _, _, err := p.open(t.Context(), testEndpoint)
	if err == nil {
		t.Fatal("open succeeded, want a data stream failure")
	}

	streams := conn.createdStreams()
	if len(streams) != 1 {
		t.Fatalf("created streams = %d, want 1 (the error stream)", len(streams))
	}
	if got := streams[0].resetCount(); got != 1 {
		t.Errorf("error stream resets = %d, want 1", got)
	}
	removed := conn.removedStreams()
	if len(removed) != 1 || removed[0] != httpstream.Stream(streams[0]) {
		t.Errorf("RemoveStreams got %v, want the error stream", removed)
	}
	// The failed open held the only reference, so eviction closed the
	// connection at once.
	if !conn.isClosed() {
		t.Error("connection not closed after its only open failed")
	}
}

func TestPoolEvictsOnCloseChan(t *testing.T) {
	f := newFakeTransport()
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	_, _, release := mustOpen(t, p, testEndpoint)
	release()

	//nolint:errcheck // the test is the one closing the connection.
	f.connAt(0).Close()
	waitFor(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.conns) == 0
	})

	_, _, release2 := mustOpen(t, p, testEndpoint)
	release2()

	if got := f.dialCount(); got != 2 {
		t.Errorf("dial count = %d, want 2", got)
	}
}

func TestPoolCreateStreamErrorKeepsSiblings(t *testing.T) {
	conn := newFakeConn()
	conn.failAt = 3 // first open uses calls 1 and 2; call 3 is the second open's error stream
	f := newFakeTransport()
	f.newConn = func() *fakeConn { return conn }
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	_, _, release := mustOpen(t, p, testEndpoint)

	_, _, _, err := p.open(t.Context(), testEndpoint)
	if err == nil {
		t.Fatal("second open succeeded, want a stream creation failure")
	}
	if conn.isClosed() {
		t.Fatal("connection closed while a sibling stream was still open")
	}

	release()
	if !conn.isClosed() {
		t.Error("connection not closed after the last stream was released")
	}
}

func TestPoolIdleClosesOnlyAtZeroStreams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		f := newFakeTransport()
		p := newPool(f.factory, idle)

		_, _, release := mustOpen(t, p, testEndpoint)
		conn := f.connAt(0)

		time.Sleep(idle + time.Second)
		if conn.isClosed() {
			t.Fatal("connection closed while a stream was still open")
		}

		release()
		time.Sleep(idle + time.Second)
		if !conn.isClosed() {
			t.Error("connection not closed after the idle timeout with no streams")
		}
		if conn.idleTimeoutSet() {
			t.Error("SetIdleTimeout was called; idle counts streams, never bytes")
		}
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

func TestPoolOpenCancelsIdleTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		f := newFakeTransport()
		p := newPool(f.factory, idle)

		_, _, release := mustOpen(t, p, testEndpoint)
		conn := f.connAt(0)
		release()

		time.Sleep(idle / 2)
		if conn.isClosed() {
			t.Fatal("connection closed before the idle timeout")
		}

		_, _, release2 := mustOpen(t, p, testEndpoint)
		time.Sleep(idle + time.Second)
		if conn.isClosed() {
			t.Fatal("connection closed while a stream was open; the timer was not cancelled")
		}

		release2()
		time.Sleep(idle + time.Second)
		if !conn.isClosed() {
			t.Error("connection not closed after the idle timeout")
		}
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

// TestPoolFirstDialHonoursContext pins F1: the caller that creates a pool entry
// must not wait out the establish timeout when its own context ends, so the dial
// runs detached and the caller waits on ready with a ctx.Done() arm. Under
// testing/synctest a dial blocked on a channel would hang the bubble for a
// synchronous dial, so reverting the fix fails this test by deadlock.
func TestPoolFirstDialHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeTransport()
		f.block = make(chan struct{})
		p := newPool(f.factory, time.Minute)

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, _, release, err := p.open(ctx, testEndpoint)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first open = %v, want context.DeadlineExceeded", err)
		}
		if release != nil {
			release()
		}

		// The detached dial is still in flight; the pool stays usable and the
		// next open takes the connection it went on to register.
		close(f.block)
		_, _, release2 := mustOpen(t, p, testEndpoint)
		release2()
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

func TestPoolOpenWaitHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeTransport()
		f.block = make(chan struct{})
		p := newPool(f.factory, time.Minute)

		dialDone := make(chan struct{})
		go func() {
			defer close(dialDone)
			_, _, release, err := p.open(t.Context(), testEndpoint)
			if err == nil {
				release()
			}
		}()
		<-f.entered // the dial is in flight and blocked

		// A context already cancelled is reported before the lock is taken.
		cctx, ccancel := context.WithCancel(t.Context())
		ccancel()
		if _, _, _, err := p.open(cctx, testEndpoint); !errors.Is(err, context.Canceled) {
			t.Errorf("open with a cancelled context = %v, want context.Canceled", err)
		}

		// A context that expires while waiting on the in-flight dial exercises
		// the select arm rather than the pre-check.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, _, _, err := p.open(ctx, testEndpoint); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("open waiting on an in-flight dial = %v, want context.DeadlineExceeded", err)
		}

		close(f.block)
		<-dialDone
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

// TestPoolIdleExpireIgnoresStaleGeneration pins the idleGen guard: a timer
// callback that fires just as an open/release cycle re-arms the idle clock must
// not close the connection the newer timer owns. Calling idleExpire with the
// older generation exercises the guard directly and deterministically, which the
// timer race itself does not allow.
func TestPoolIdleExpireIgnoresStaleGeneration(t *testing.T) {
	f := newFakeTransport()
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	_, _, release := mustOpen(t, p, testEndpoint)
	release()
	conn := f.connAt(0)

	p.mu.Lock()
	pc := p.conns[testEndpoint.Namespace+"/"+testEndpoint.Pod]
	staleGen := pc.idleGen
	p.mu.Unlock()

	// A later open and release re-arms the idle clock, advancing the generation.
	_, _, release2 := mustOpen(t, p, testEndpoint)
	release2()

	p.mu.Lock()
	currentGen := pc.idleGen
	p.mu.Unlock()
	if staleGen == currentGen {
		t.Fatalf("idle generation did not advance past %d", staleGen)
	}

	p.idleExpire(pc, staleGen)
	if conn.isClosed() {
		t.Error("a stale idle callback closed the connection after a re-arm")
	}
}

// TestPoolReleaseIsIdempotent pins the sync.Once around open's release: a caller
// that releases twice must not drop a sibling's reference and let the idle timer
// close a connection still in use.
func TestPoolReleaseIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		f := newFakeTransport()
		p := newPool(f.factory, idle)

		_, _, releaseA := mustOpen(t, p, testEndpoint)
		_, _, releaseB := mustOpen(t, p, testEndpoint)
		conn := f.connAt(0)

		releaseA()
		releaseA() // must be a no-op, not a second decrement

		time.Sleep(idle + time.Second)
		if conn.isClosed() {
			t.Fatal("a double release dropped a sibling's reference")
		}

		releaseB()
		time.Sleep(idle + time.Second)
		if !conn.isClosed() {
			t.Error("connection not closed after the real last release")
		}
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

func TestPoolCloseThenOpenStopped(t *testing.T) {
	f := newFakeTransport()
	p := newPool(f.factory, time.Minute)

	_, _, release := mustOpen(t, p, testEndpoint)
	release()

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}

	if _, _, _, err := p.open(t.Context(), testEndpoint); !errors.Is(err, ErrStopped) {
		t.Errorf("open after Close = %v, want ErrStopped", err)
	}
	if !f.connAt(0).isClosed() {
		t.Error("connection not closed by Close")
	}
}

func TestNewRESTStreamDialerFactoryRejectsBadTimeout(t *testing.T) {
	cfg := &rest.Config{Host: "http://127.0.0.1:1"}
	if _, err := newRESTStreamDialerFactory(cfg, nil, 0); err == nil {
		t.Fatal("newRESTStreamDialerFactory with a non-positive timeout = nil error, want an error")
	}
}

// TestRESTStreamDialerFactoryBoundsWebsocketDial pins the websocket establish
// bound: client-go's websocket transport ignores rest.Config.Dial and reaches
// the network through a context.Background() request, so only wrapping the
// transport bounds it. A server that accepts and never answers must not hang
// the dial.
func TestRESTStreamDialerFactoryBoundsWebsocketDial(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		//nolint:errcheck // the listener is being torn down by the test.
		ln.Close()
	}()

	hold := make(chan struct{})
	defer close(hold)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				<-hold
				//nolint:errcheck // the connection is being torn down by the test.
				conn.Close()
			}()
		}
	}()

	cfg := &rest.Config{Host: "http://" + ln.Addr().String()}
	core, err := corev1client.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("core client: %v", err)
	}

	const timeout = 200 * time.Millisecond
	factory, err := newRESTStreamDialerFactory(cfg, core, timeout)
	if err != nil {
		t.Fatalf("newRESTStreamDialerFactory: %v", err)
	}
	dialer, err := factory("ns", "pod")
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	start := time.Now()
	conn, _, err := dialer.Dial(portforward.PortForwardProtocolV1Name)
	elapsed := time.Since(start)
	if err == nil {
		//nolint:errcheck // the test fails next; the connection is irrelevant.
		conn.Close()
		t.Fatal("Dial succeeded against a server that never answers")
	}
	if elapsed > 2*time.Second {
		t.Errorf("Dial took %v, want it bounded near %v", elapsed, timeout)
	}
}

func TestPoolFailedDialThenOpenSucceeds(t *testing.T) {
	f := newFakeTransport()
	f.err = errors.New("dial refused")
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	if _, _, _, err := p.open(t.Context(), testEndpoint); err == nil {
		t.Fatal("open succeeded, want the dial error")
	}

	f.mu.Lock()
	f.err = nil
	f.mu.Unlock()

	_, _, release := mustOpen(t, p, testEndpoint)
	release()

	if got := f.dialCount(); got != 2 {
		t.Errorf("dial count = %d, want 2", got)
	}
}

// TestPoolConcurrentFailedDialShared checks that the callers waiting on one
// in-flight dial receive its failure, rather than each starting a fresh dial.
func TestPoolConcurrentFailedDialShared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeTransport()
		f.block = make(chan struct{})
		f.err = errors.New("dial refused")
		p := newPool(f.factory, time.Minute)
		defer closePool(t, p)

		const n = 8
		errs := make(chan error, n)
		var wg sync.WaitGroup
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, _, err := p.open(t.Context(), testEndpoint)
				errs <- err
			}()
		}

		synctest.Wait()
		close(f.block)
		wg.Wait()
		close(errs)

		for err := range errs {
			if !errors.Is(err, f.err) {
				t.Errorf("open error = %v, want the shared dial error %v", err, f.err)
			}
		}
		if got := f.dialCount(); got != 1 {
			t.Errorf("dial count = %d, want 1 (the failure is shared, not retried)", got)
		}
	})
}

func TestPoolRedialsWhileEvictedConnStillLive(t *testing.T) {
	connA := newFakeConn()
	connA.failAt = 3 // the second open's error stream
	connB := newFakeConn()

	f := newFakeTransport()
	f.queue = []*fakeConn{connA, connB}
	p := newPool(f.factory, time.Minute)
	defer closePool(t, p)

	// The first open holds a stream on connA.
	_, _, releaseA := mustOpen(t, p, testEndpoint)
	// The second open fails to create a stream and evicts connA, but releaseA
	// keeps connA alive.
	if _, _, _, err := p.open(t.Context(), testEndpoint); err == nil {
		t.Fatal("second open succeeded, want a stream creation failure")
	}
	if connA.isClosed() {
		t.Fatal("connA closed while its sibling stream was still open")
	}

	// A third open must dial connB rather than reuse the evicted connA.
	_, _, releaseC := mustOpen(t, p, testEndpoint)
	if got := f.dialCount(); got != 2 {
		t.Errorf("dial count = %d, want 2", got)
	}
	releaseC()

	releaseA()
	if !connA.isClosed() {
		t.Error("connA not closed after its last stream was released")
	}
}

func TestPoolDialFinishingAfterCloseStops(t *testing.T) {
	f := newFakeTransport()
	f.block = make(chan struct{})
	p := newPool(f.factory, time.Minute)

	done := make(chan error, 1)
	go func() {
		_, _, _, err := p.open(t.Context(), testEndpoint)
		done <- err
	}()
	<-f.entered // the dial is in flight

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(f.block) // the dial now completes against a closed pool

	if err := <-done; !errors.Is(err, ErrStopped) {
		t.Errorf("open finishing after Close = %v, want ErrStopped", err)
	}
	if !f.connAt(0).isClosed() {
		t.Error("the late dial's connection was not closed")
	}
}
