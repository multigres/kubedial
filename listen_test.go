package kubedial

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	httpstream "k8s.io/streaming/pkg/httpstream"
)

// listenTestTransport is a streamDialerFactory whose connections hand back
// scriptedStream pairs, so a test can drive the pod side of every stream.
type listenTestTransport struct {
	mu      sync.Mutex
	dials   int
	pods    []string
	conns   []*listenTestConn
	forward string // written on each error stream, e.g. a kubelet refusal
	lossErr error  // read on each data stream, e.g. a dropped port-forward
	dialErr error  // returned from Dial, simulating a failed stream open

	block       chan struct{} // when set, Dial waits on it before returning
	entered     chan struct{} // closed once Dial is waiting on block
	enteredOnce sync.Once
}

func newListenTestTransport() *listenTestTransport { return &listenTestTransport{} }

func (t *listenTestTransport) factory(ns, pod string) (httpstream.Dialer, error) {
	return &listenTestDialer{t: t, pod: ns + "/" + pod}, nil
}

func (t *listenTestTransport) dialCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dials
}

func (t *listenTestTransport) dialedPods() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.pods...)
}

func (t *listenTestTransport) dataStreams() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, c := range t.conns {
		n += c.dataCount()
	}
	return n
}

// dataServer returns the i-th data stream's pod side across all connections.
func (t *listenTestTransport) dataServer(i int) *testStreamServer {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, c := range t.conns {
		c.mu.Lock()
		for _, srv := range c.dataSrv {
			if n == i {
				c.mu.Unlock()
				return srv
			}
			n++
		}
		c.mu.Unlock()
	}
	return nil
}

type listenTestDialer struct {
	t   *listenTestTransport
	pod string
}

func (d *listenTestDialer) Dial(...string) (httpstream.Connection, string, error) {
	t := d.t
	if t.block != nil {
		t.enteredOnce.Do(func() { close(t.entered) })
		<-t.block
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dialErr != nil {
		return nil, "", t.dialErr
	}
	t.dials++
	t.pods = append(t.pods, d.pod)
	conn := newListenTestConn(t.forward, t.lossErr)
	t.conns = append(t.conns, conn)
	return conn, portforward.PortForwardProtocolV1Name, nil
}

// listenTestConn is an httpstream.Connection whose streams are backed by
// scriptedStream pairs. With forward empty the data stream stays open, so a
// test can push bytes through; with forward set every data stream ends and
// every error stream carries the message, the shape of a kubelet refusal.
type listenTestConn struct {
	mu      sync.Mutex
	forward string
	lossErr error
	created int
	dataSrv []*testStreamServer
	closeCh chan bool
	once    sync.Once
}

func newListenTestConn(forward string, lossErr error) *listenTestConn {
	return &listenTestConn{forward: forward, lossErr: lossErr, closeCh: make(chan bool)}
}

// listenTestLossStream is a data stream whose read fails with a non-EOF error,
// the shape of a port-forward connection dropping mid-copy.
type listenTestLossStream struct {
	*scriptedStream
	err error
}

func (s *listenTestLossStream) Read([]byte) (int, error) { return 0, s.err }

func (c *listenTestConn) CreateStream(h http.Header) (httpstream.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created++
	client, srv := newStreamPair(uint32(c.created))
	if h.Get(corev1.StreamType) == corev1.StreamTypeError {
		msg := c.forward
		go func() {
			if msg != "" {
				_, _ = srv.w.Write([]byte(msg))
			}
			_ = srv.w.Close()
		}()
		return client, nil
	}
	c.dataSrv = append(c.dataSrv, srv)
	if c.forward != "" {
		// A refusal ends the data stream as well, which is what lets the
		// streamConn surface the error message in place of io.EOF.
		_ = srv.w.Close()
	}
	if c.lossErr != nil {
		return &listenTestLossStream{scriptedStream: client, err: c.lossErr}, nil
	}
	return client, nil
}

func (c *listenTestConn) Close() error {
	c.once.Do(func() { close(c.closeCh) })
	return nil
}

func (c *listenTestConn) CloseChan() <-chan bool { return c.closeCh }

func (c *listenTestConn) SetIdleTimeout(time.Duration) {}

func (c *listenTestConn) RemoveStreams(...httpstream.Stream) {}

func (c *listenTestConn) dataCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.dataSrv)
}

// listenTestForwardDialer returns a forward-mode Dialer whose pool is backed by
// the scripted transport, plus the transport and the client call counter.
func listenTestForwardDialer(t *testing.T, podIP string) (*Dialer, *listenTestTransport, *recordingClients) {
	t.Helper()
	tr := newListenTestTransport()
	clients := webServiceClients(podIP)
	d, err := New(&rest.Config{Host: "https://example.invalid"},
		WithMode(ModeForward),
		withClients(clients),
		withStreamDialerFactory(tr.factory),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, tr, clients
}

func listenTestDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	var d net.Dialer
	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dial listener %s: %v", addr, err)
	}
	return conn
}

func TestListenAddrStable(t *testing.T) {
	d, _, _ := listenTestForwardDialer(t, "10.0.0.7")
	l, err := d.Listen(t.Context(), "web.ns:80")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	addr := l.Addr()
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatalf("Addr = %q, want a loopback address", addr)
	}
	if got := l.Addr(); got != addr {
		t.Errorf("Addr changed before any connection: %q then %q", addr, got)
	}

	conn := listenTestDial(t, addr)
	defer conn.Close()
	if got := l.Addr(); got != addr {
		t.Errorf("Addr changed after a connection: %q then %q", addr, got)
	}
}

func TestListenRedialsPerConnection(t *testing.T) {
	d, tr, clients := listenTestForwardDialer(t, "10.0.0.7")
	// A zero TTL makes every dial resolve again, which is what proves the
	// listener resolves per connection rather than once.
	d.cache = newResolveCache(time.Now, 0, 0)

	l, err := d.Listen(t.Context(), "web.ns:80")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	first := listenTestDial(t, l.Addr())
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	callsAfterFirst := clients.callCount()
	first.Close()

	second := listenTestDial(t, l.Addr())
	waitFor(t, func() bool { return tr.dataStreams() == 2 })
	second.Close()

	if callsAfterFirst == 0 {
		t.Error("the first connection did not resolve through the clients")
	}
	if got := clients.callCount(); got <= callsAfterFirst {
		t.Errorf("clients called %d times after the second connection, want more than %d: the address was not resolved again", got, callsAfterFirst)
	}
}

func TestListenEndpointPinned(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	l, err := d.ListenEndpoint(t.Context(), testEndpoint)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	for i := 0; i < 3; i++ {
		conn := listenTestDial(t, l.Addr())
		waitFor(t, func() bool { return tr.dataStreams() == i+1 })
		conn.Close()
	}

	if got := tr.dialCount(); got != 1 {
		t.Errorf("pool dials = %d, want 1: all connections share the pod's forward", got)
	}
	for _, pod := range tr.dialedPods() {
		if pod != "ns/pod" {
			t.Errorf("connection reached %q, want the pinned pod %q", pod, "ns/pod")
		}
	}
}

// TestListenReadyCheckWarmsFirstConnection pins what the ready check is for:
// the probe must leave the pooled forward in place so the first accepted
// connection reuses it rather than paying a second upgrade. It also pins that
// the probe's fast teardown releases the pool reference.
func TestListenReadyCheckWarmsFirstConnection(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithReadyCheck(100*time.Millisecond))
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	if got := tr.dialCount(); got != 1 {
		t.Fatalf("pool dials after the ready check = %d, want 1", got)
	}
	before := tr.dataStreams()

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()
	waitFor(t, func() bool { return tr.dataStreams() == before+1 })

	if got := tr.dialCount(); got != 1 {
		t.Errorf("pool dials after the first accepted connection = %d, want 1: the ready check did not warm the forward", got)
	}
}

func TestListenNoRedialStopsOnForwardError(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	tr.forward = "connection refused"

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithNoRedial())
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()

	listenTestWaitDone(t, l)

	var ferr *ForwardError
	if !errors.As(l.Err(), &ferr) {
		t.Fatalf("Err = %v (%T), want a *ForwardError", l.Err(), l.Err())
	}
	if ferr.Msg != "connection refused" {
		t.Errorf("ForwardError.Msg = %q, want %q", ferr.Msg, "connection refused")
	}
}

func TestListenNoRedialStopsOnOpenFailure(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	tr.dialErr = errors.New("boom")

	dialErrs := make(chan error, 1)
	l, err := d.ListenEndpoint(t.Context(), testEndpoint,
		WithNoRedial(),
		WithOnDialError(func(e error) { dialErrs <- e }),
	)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()

	listenTestWaitDone(t, l)

	if l.Err() == nil {
		t.Fatal("Err = nil, want the stream-open failure")
	}
	if !strings.Contains(l.Err().Error(), "boom") {
		t.Errorf("Err = %v, want it to carry the open failure", l.Err())
	}
	select {
	case got := <-dialErrs:
		if !strings.Contains(got.Error(), "boom") {
			t.Errorf("onDialError = %v, want it to carry the open failure", got)
		}
	default:
		t.Error("onDialError was not called for the failed dial")
	}
}

func TestListenWithoutNoRedialSurvivesForwardError(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	tr.forward = "connection refused"

	l, err := d.ListenEndpoint(t.Context(), testEndpoint)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	for i := 0; i < 2; i++ {
		conn := listenTestDial(t, l.Addr())
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		// The forward error ends the connection; the listener does not close it
		// to the caller as an orderly end of stream.
		if _, err := conn.Read(make([]byte, 1)); err == nil {
			t.Fatalf("read %d succeeded, want the forward error to end the connection", i)
		}
		conn.Close()
	}

	select {
	case <-l.Done():
		t.Fatalf("listener stopped after a forward error without WithNoRedial: %v", l.Err())
	default:
	}
	if l.Err() != nil {
		t.Errorf("Err = %v while still running, want nil", l.Err())
	}
}

func TestListenNoRedialStopsOnCleanUpstreamEnd(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithNoRedial())
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	// The client stays connected: the pod ending the stream on its own, with no
	// error and no kubelet message, is the shape of a graceful failover when it
	// leaves no other stream on the connection, and must stop a pinned listener.
	conn := listenTestDial(t, l.Addr())
	defer conn.Close()
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	srv := tr.dataServer(0)
	if srv == nil {
		t.Fatal("no data stream server")
	}
	if err := srv.w.Close(); err != nil {
		t.Fatalf("pod close: %v", err)
	}

	listenTestWaitDone(t, l)

	if l.Err() == nil {
		t.Fatal("Err = nil, want a non-nil reason for the clean upstream end")
	}
	if !errors.Is(l.Err(), ErrStopped) {
		t.Errorf("Err = %v, want errors.Is(err, ErrStopped)", l.Err())
	}
}

// TestListenNoRedialStopsOnlyWhenNoSiblingRemains pins the two halves of the
// WithNoRedial stop condition at once: one stream ending on a live pod (a
// Postgres cancel request, a server-side terminate) must not stop the
// listener, and the last stream ending must. It fails whichever half is
// removed.
func TestListenNoRedialStopsOnlyWhenNoSiblingRemains(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithNoRedial())
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	// Two streams on the one pinned pod's port-forward connection: a session
	// and the short-lived connection a cancel request arrives on.
	first := listenTestDial(t, l.Addr())
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	second := listenTestDial(t, l.Addr())
	defer second.Close()
	waitFor(t, func() bool { return tr.dataStreams() == 2 })

	// The pod closes the first stream while its client is still connected, the
	// shape of a cancel request. Bring that stream fully down (its client closes
	// too) and wait for its reference to be released; the second stream is still
	// live, so the pod's forward is not gone and the listener must keep running.
	if err := tr.dataServer(0).w.Close(); err != nil {
		t.Fatalf("pod close first stream: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first client close: %v", err)
	}
	// One reference left while running; zero if the listener wrongly stopped and
	// tore the sibling down with it.
	waitFor(t, func() bool { return listenTestPoolRefs(d, testEndpoint) <= 1 })
	select {
	case <-l.Done():
		t.Fatalf("listener stopped after one stream ended while a sibling was live: %v", l.Err())
	default:
	}
	if l.Err() != nil {
		t.Errorf("Err = %v while still running, want nil", l.Err())
	}

	// End the last stream: no sibling remains, so the pinned forward is gone and
	// the listener must stop.
	if err := tr.dataServer(1).w.Close(); err != nil {
		t.Fatalf("pod close second stream: %v", err)
	}
	listenTestWaitDone(t, l)
	if l.Err() == nil {
		t.Fatal("Err = nil, want a non-nil reason once the last stream ended")
	}
}

// TestListenNoRedialSiblingKeepsListenerRunning pins R1: the Listen path's
// sibling callback must use the same post-release semantics as ListenEndpoint's.
// One of two live streams ending cleanly leaves a sibling holding the pod's
// forward, so the listener must keep running; the last one ending must stop it.
// A pre-release refs > 1 test reads the one remaining reference as "no sibling"
// and stops the listener on the first stream's clean end.
func TestListenNoRedialSiblingKeepsListenerRunning(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.Listen(t.Context(), "web.ns:80", WithNoRedial())
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	// web.ns:80 resolves to web-1 through the shared fixture, so that is the
	// pool key the reference count is read under.
	ep := Endpoint{Namespace: "ns", Pod: "web-1", PodIP: "10.0.0.7", Port: 8080}

	first := listenTestDial(t, l.Addr())
	defer first.Close()
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	second := listenTestDial(t, l.Addr())
	defer second.Close()
	waitFor(t, func() bool { return tr.dataStreams() == 2 })

	// The pod ends the first stream while its client and its sibling stay
	// connected, the shape of a cancel request on a live pod.
	if err := tr.dataServer(0).w.Close(); err != nil {
		t.Fatalf("pod close first stream: %v", err)
	}
	// The ending stream's reference is released; one sibling remains.
	waitFor(t, func() bool { return listenTestPoolRefs(d, ep) <= 1 })
	select {
	case <-l.Done():
		t.Fatalf("listener stopped after one stream ended while a sibling was live: %v", l.Err())
	default:
	}
	if l.Err() != nil {
		t.Errorf("Err = %v while still running, want nil", l.Err())
	}

	// The last stream ending leaves no sibling, so the pinned forward is gone.
	if err := tr.dataServer(1).w.Close(); err != nil {
		t.Fatalf("pod close second stream: %v", err)
	}
	listenTestWaitDone(t, l)
	if l.Err() == nil {
		t.Fatal("Err = nil, want a non-nil reason once the last stream ended")
	}
}

// TestListenNoRedialStopsWhenTwoStreamsEndTogether pins the race the pre-release
// sibling check lost: two streams on one pinned forward ending at the same
// instant must still stop the listener. Each pipe releases its own reference
// before asking, so whichever ends last sees none and stops. Both pod sides are
// ended while both clients are still connected, so neither pipe's teardown can
// release a reference ahead of its own check: under a check made before the
// release they each see the other as a sibling and neither stops.
func TestListenNoRedialStopsWhenTwoStreamsEndTogether(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithNoRedial())
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	first := listenTestDial(t, l.Addr())
	defer first.Close()
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	second := listenTestDial(t, l.Addr())
	defer second.Close()
	waitFor(t, func() bool { return tr.dataStreams() == 2 })

	// Both backends end together, the shape of a Postgres restart during an
	// in-place demotion killing every stream on the pod's forward at once.
	if err := tr.dataServer(0).w.Close(); err != nil {
		t.Fatalf("pod close first stream: %v", err)
	}
	if err := tr.dataServer(1).w.Close(); err != nil {
		t.Fatalf("pod close second stream: %v", err)
	}

	listenTestWaitDone(t, l)
	if l.Err() == nil {
		t.Fatal("Err = nil, want a non-nil reason once the pinned forward ended")
	}
}

// listenTestPoolRefs reports how many live references the pool holds on the
// connection for ep, which is what the listener's sibling check reads.
func listenTestPoolRefs(d *Dialer, ep Endpoint) int {
	d.pool.mu.Lock()
	defer d.pool.mu.Unlock()
	pc, ok := d.pool.conns[ep.Namespace+"/"+ep.Pod]
	if !ok {
		return 0
	}
	return pc.refs
}

// orderConn is a net.Conn that records when its Close, CloseWrite and
// OtherStreamsOpen run, so a test can pin the order pipe uses them in.
type orderConn struct {
	net.Conn

	record  func(tag, event string)
	tag     string
	sibling bool
	told    chan struct{}
	once    sync.Once
}

func (c *orderConn) CloseWrite() error {
	c.record(c.tag, "CloseWrite")
	if c.told != nil {
		c.once.Do(func() { close(c.told) })
	}
	return nil
}

func (c *orderConn) Close() error {
	c.record(c.tag, "Close")
	return c.Conn.Close()
}

func (c *orderConn) OtherStreamsOpen() bool {
	c.record(c.tag, "OtherStreamsOpen")
	return c.sibling
}

// TestPipeReleasesBeforeTellingClient pins R3: on a clean upstream-first end the
// ending stream is released and the sibling check runs before the client is told
// (CloseWrite). Telling the client first would let a client that reconnects on
// EOF take a reference on the same pooled connection before the check, look like
// a sibling, and suppress the stop.
func TestPipeReleasesBeforeTellingClient(t *testing.T) {
	upOuter, upInner := net.Pipe()
	clOuter, clInner := net.Pipe()
	defer upInner.Close()
	defer clInner.Close()

	var mu sync.Mutex
	var events []string
	record := func(tag, event string) {
		mu.Lock()
		events = append(events, tag+"."+event)
		mu.Unlock()
	}

	client := &orderConn{Conn: clOuter, record: record, tag: "client", told: make(chan struct{})}
	upstream := &orderConn{Conn: upOuter, record: record, tag: "upstream"}
	upstream.sibling = true // a sibling is live, so no stop is attempted

	l := &Listener{noRedial: true, conns: map[net.Conn]struct{}{}}
	l.wg.Add(1)
	go l.pipe(client, upstream)

	// The pod ends the stream while the client is still connected.
	if err := upInner.Close(); err != nil {
		t.Fatalf("pod close: %v", err)
	}
	select {
	case <-client.told:
	case <-time.After(5 * time.Second):
		t.Fatal("client was never told the upstream ended")
	}

	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	want := []string{"upstream.Close", "upstream.OtherStreamsOpen", "client.CloseWrite"}
	if len(got) != len(want) {
		t.Fatalf("pipe events when the client was told = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pipe events when the client was told = %v, want %v", got, want)
		}
	}

	// Let the client -> upstream copy finish so pipe returns.
	if err := clInner.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	l.wg.Wait()
}

func TestListenNoRedialSurvivesClientDisconnect(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithNoRedial())
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	conn := listenTestDial(t, l.Addr())
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	srv := tr.dataServer(0)
	if srv == nil {
		t.Fatal("no data stream server")
	}

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	if _, err := io.ReadFull(srv.r, make([]byte, 4)); err != nil {
		t.Fatalf("pod read: %v", err)
	}

	// The client disconnects first. The upstream end that follows is a
	// consequence, so the listener must keep running for the next connection.
	conn.Close()
	if err := srv.r.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := srv.r.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("pod read after client disconnect = %v, want io.EOF", err)
	}
	if err := srv.w.Close(); err != nil {
		t.Fatalf("pod close: %v", err)
	}

	waitFor(t, func() bool { return l.activeConns() == 0 })

	select {
	case <-l.Done():
		t.Fatalf("listener stopped after the client disconnected first: %v", l.Err())
	default:
	}
	if l.Err() != nil {
		t.Errorf("Err = %v while still running, want nil", l.Err())
	}
}

func TestListenReadyCheckRefused(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	tr.forward = "connection refused"

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithReadyCheck(2*time.Second))
	if err == nil {
		_ = l.Close()
		t.Fatal("ListenEndpoint = nil error, want ErrNotReady")
	}
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("err = %v, want errors.Is(err, ErrNotReady)", err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("err = %v, want it to render the kubelet message", err)
	}
}

func TestListenReadyCheckOK(t *testing.T) {
	d, _, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithReadyCheck(100*time.Millisecond))
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestListenReadyCheckConnectionLoss(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	loss := errors.New("port-forward connection lost")
	tr.lossErr = loss

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithReadyCheck(2*time.Second))
	if err == nil {
		_ = l.Close()
		t.Fatal("ListenEndpoint = nil error, want ErrNotReady for a lost connection")
	}
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("err = %v, want errors.Is(err, ErrNotReady)", err)
	}
	if !strings.Contains(err.Error(), loss.Error()) {
		t.Errorf("err = %v, want it to render the connection loss", err)
	}
}

func TestListenOnDialErrorSilentOnShutdown(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	tr.block = make(chan struct{})
	tr.entered = make(chan struct{})
	// The pool reports a cancelled acquire as a plain context.Canceled, which is
	// what the accept loop must not mistake for a dial failure.
	tr.dialErr = context.Canceled

	dialErrs := make(chan error, 1)
	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithOnDialError(func(e error) { dialErrs <- e }))
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()
	<-tr.entered

	closeErr := make(chan error, 1)
	go func() { closeErr <- l.Close() }()
	// Wait until shutdown has begun so the dial failure is unambiguously the
	// listener's own stop, then let the blocked dial return.
	waitFor(t, l.isStopping)
	close(tr.block)
	listenTestWaitDone(t, l)
	if err := <-closeErr; err != nil {
		t.Errorf("Close: %v", err)
	}

	select {
	case got := <-dialErrs:
		t.Errorf("onDialError = %v, want none: the dial failure was the listener's own shutdown", got)
	default:
	}
}

// TestListenStalledDialDoesNotBlockAccept pins that the upstream dial runs on
// the accepted connection's own goroutine: with the first connection's dial
// blocked, the accept loop still takes a second connection rather than leaving
// it in the backlog, and the listener still stops on the first stream-open
// failure once the blocked dial returns under WithNoRedial.
func TestListenStalledDialDoesNotBlockAccept(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	tr.block = make(chan struct{})
	tr.entered = make(chan struct{})
	tr.dialErr = errors.New("boom")

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(tr.block) }) }

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithNoRedial())
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()
	// The fake dial ignores its context, so a blocked one is freed here even if
	// the test fails before the explicit release, or Close would never join it.
	defer release()

	first := listenTestDial(t, l.Addr())
	defer first.Close()
	<-tr.entered // the first connection's dial is in flight and blocked

	// The accept loop is free, so it takes the second connection and starts its
	// dial instead of leaving it queued behind the first.
	second := listenTestDial(t, l.Addr())
	defer second.Close()
	waitFor(t, func() bool { return l.activeConns() == 2 })

	// Release the dials; they fail, and the first failure stops the pinned
	// listener.
	release()
	listenTestWaitDone(t, l)
	if l.Err() == nil {
		t.Fatal("Err = nil, want the stream-open failure")
	}
	if !strings.Contains(l.Err().Error(), "boom") {
		t.Errorf("Err = %v, want it to carry the open failure", l.Err())
	}
}

func TestListenStopsWhenDialerClosed(t *testing.T) {
	d, _, _ := listenTestForwardDialer(t, "10.0.0.7")
	l, err := d.ListenEndpoint(t.Context(), testEndpoint)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Dialer.Close: %v", err)
	}

	// Every future dial now fails with ErrStopped; the listener must stop rather
	// than fail connections forever with Done never closing.
	conn := listenTestDial(t, l.Addr())
	defer conn.Close()
	listenTestWaitDone(t, l)

	if !errors.Is(l.Err(), ErrStopped) {
		t.Errorf("Err = %v, want errors.Is(err, ErrStopped)", l.Err())
	}
}

func TestListenStopsWithContext(t *testing.T) {
	d, _, _ := listenTestForwardDialer(t, "10.0.0.7")

	ctx, cancel := context.WithCancel(t.Context())
	l, err := d.ListenEndpoint(ctx, testEndpoint)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	cancel()
	listenTestWaitDone(t, l)

	if !errors.Is(l.Err(), ErrStopped) {
		t.Errorf("Err = %v, want errors.Is(err, ErrStopped)", l.Err())
	}
	if errors.Is(l.Err(), context.Canceled) {
		t.Errorf("Err = %v, must not chain the context error", l.Err())
	}
	if !strings.Contains(l.Err().Error(), context.Canceled.Error()) {
		t.Errorf("Err = %v, want the context error rendered into the message", l.Err())
	}
}

func TestListenHTTPClientDialsAddr(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	defer backend.Close()

	d, err := New(nil, WithMode(ModeDirect))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l, err := d.Listen(t.Context(), strings.TrimPrefix(backend.URL, "http://"))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	// The URL names a host that does not resolve: the client must dial the
	// listener regardless of it.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://in-cluster.example.invalid/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := l.HTTPClient(nil).Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Errorf("response = %d %q, want 200 %q", resp.StatusCode, body, "hello")
	}
}

func TestListenCloseNoGoroutineLeak(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	base := runtime.NumGoroutine()

	l, err := d.ListenEndpoint(t.Context(), testEndpoint)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	// Hold one connection open across Close so stop has a live copy to tear
	// down, and open a second that the client closes itself.
	held := listenTestDial(t, l.Addr())
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	other := listenTestDial(t, l.Addr())
	waitFor(t, func() bool { return tr.dataStreams() == 2 })
	other.Close()

	if err := l.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// Close must not return until every goroutine it owns has finished, so Done
	// has to be closed already. A polling check would pass a Close that never
	// joined, which is what this asserts.
	select {
	case <-l.Done():
	default:
		t.Error("Close returned before the listener had stopped and joined its goroutines")
	}

	// The held connection was torn down by the listener, not by the client.
	if err := held.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := held.Read(make([]byte, 1)); err == nil {
		t.Error("held connection read succeeded after Close, want it torn down")
	}
	held.Close()

	if err := d.Close(); err != nil {
		t.Errorf("Dialer.Close: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines: %d, want <= %d after Close", runtime.NumGoroutine(), base)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// activeConns reports how many connections the listener is still copying. It is
// a test observable for teardown: pipe untracks both ends only once both
// directions have finished.
func (l *Listener) activeConns() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.conns)
}

func TestListenUpstreamErrorClosesBoth(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	tr.forward = "connection refused"

	l, err := d.ListenEndpoint(t.Context(), testEndpoint)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	// The client is deliberately left open: an upstream error must close both
	// ends rather than half-close the client and wait for it.
	conn := listenTestDial(t, l.Addr())
	defer conn.Close()

	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	waitFor(t, func() bool { return l.activeConns() == 0 })
}

func TestListenLocalPort(t *testing.T) {
	// Find a free port, release it, then ask the listener for exactly it.
	var lc net.ListenConfig
	probe, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatalf("probe close: %v", err)
	}

	d, _, _ := listenTestForwardDialer(t, "10.0.0.7")
	l, err := d.Listen(t.Context(), "web.ns:80", WithLocalPort(uint16(port)))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	want := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	if got := l.Addr(); got != want {
		t.Errorf("Addr = %q, want %q", got, want)
	}
}

func TestListenNoRedialStopsOnConnectionLoss(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")
	loss := errors.New("port-forward connection lost")
	tr.lossErr = loss

	l, err := d.ListenEndpoint(t.Context(), testEndpoint, WithNoRedial())
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()

	listenTestWaitDone(t, l)

	if !errors.Is(l.Err(), loss) {
		t.Errorf("Err = %v, want it to carry the connection loss", l.Err())
	}
}

func TestListenHalfClose(t *testing.T) {
	d, tr, _ := listenTestForwardDialer(t, "10.0.0.7")

	l, err := d.ListenEndpoint(t.Context(), testEndpoint)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() { _ = l.Close() }()

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()
	waitFor(t, func() bool { return tr.dataStreams() == 1 })
	srv := tr.dataServer(0)
	if srv == nil {
		t.Fatal("no data stream server")
	}

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(srv.r, buf); err != nil {
		t.Fatalf("pod read: %v", err)
	}
	if string(buf) != "ping" {
		t.Errorf("pod read %q, want %q", buf, "ping")
	}

	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("client conn is %T, want *net.TCPConn", conn)
	}
	if err := tcp.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}
	if _, err := srv.r.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("pod read after client CloseWrite = %v, want io.EOF", err)
	}

	// The pod's reply must still reach the client: only the client's write side
	// was closed.
	if _, err := srv.w.Write([]byte("pong")); err != nil {
		t.Fatalf("pod write: %v", err)
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("client read after CloseWrite: %v", err)
	}
	if string(buf) != "pong" {
		t.Errorf("client read %q, want %q", buf, "pong")
	}
}

func TestListenEndpointDirectModeErrors(t *testing.T) {
	d, err := New(nil, WithMode(ModeDirect))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := d.ListenEndpoint(t.Context(), testEndpoint); err == nil {
		t.Fatal("ListenEndpoint in direct mode = nil error, want an error rather than a nil pool deref")
	}
}

func TestListenerDoRetriesIdempotentOnce(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()

	d, err := New(nil, WithMode(ModeDirect))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l, err := d.Listen(t.Context(), strings.TrimPrefix(backend.URL, "http://"))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	rt := &countingRT{failFirst: 1}
	client := &http.Client{Transport: rt}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://in-cluster.example.invalid/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := l.Do(t.Context(), client, req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if got := rt.count(); got != 2 {
		t.Errorf("attempts = %d, want 2 (one retry)", got)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestListenerDoNeverRetriesPost(t *testing.T) {
	d, err := New(nil, WithMode(ModeDirect))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l, err := d.Listen(t.Context(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	rt := &countingRT{failFirst: 1}
	client := &http.Client{Transport: rt}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://in-cluster.example.invalid/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := l.Do(t.Context(), client, req)
	if err == nil {
		t.Fatal("Do = nil error, want the first failure")
	}
	if resp != nil {
		resp.Body.Close()
	}
	if got := rt.count(); got != 1 {
		t.Errorf("attempts = %d, want 1: POST is never retried", got)
	}
}

func TestListenerDoContextEndedForbidsRetry(t *testing.T) {
	d, err := New(nil, WithMode(ModeDirect))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l, err := d.Listen(t.Context(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rt := &countingRT{failFirst: 1}
	client := &http.Client{Transport: rt}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://in-cluster.example.invalid/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := l.Do(ctx, client, req)
	if resp != nil {
		resp.Body.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want errors.Is(err, context.Canceled)", err)
	}
	if got := rt.count(); got != 1 {
		t.Errorf("attempts = %d, want 1: a dead ctx forbids the retry", got)
	}
}

// TestListenerDoFailedRetryReportsBothErrors pins that Listener.Do reports the
// first attempt's error as well as the retry's, the contract Dialer.Do already
// keeps, rather than dropping the first attempt.
func TestListenerDoFailedRetryReportsBothErrors(t *testing.T) {
	d, err := New(nil, WithMode(ModeDirect))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	l, err := d.Listen(t.Context(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	first := errors.New("fake: first attempt failed")
	second := errors.New("fake: retry failed")
	rt := &twoErrRT{first: first, second: second}
	client := &http.Client{Transport: rt}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://in-cluster.example.invalid/", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := l.Do(t.Context(), client, req)
	if resp != nil {
		defer resp.Body.Close()
		t.Errorf("Do response = %v, want nil", resp)
	}
	if err == nil {
		t.Fatal("Do = nil error, want both attempts' failures")
	}
	if !errors.Is(err, first) {
		t.Errorf("Do error = %v, want errors.Is(..., first attempt error)", err)
	}
	if !errors.Is(err, second) {
		t.Errorf("Do error = %v, want errors.Is(..., retry error)", err)
	}
}

func listenTestWaitDone(t *testing.T, l *Listener) {
	t.Helper()
	select {
	case <-l.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not stop in time")
	}
}
