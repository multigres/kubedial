package kubedial

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/net/nettest"
	"k8s.io/streaming/pkg/httpstream"
)

var _ httpstream.Stream = (*scriptedStream)(nil)

// scriptedStream is one end of a scriptable httpstream.Stream. Reads and writes
// go to separate net.Pipes so Close can half-close the write direction the way
// a real port-forward stream does, while reads keep flowing.
//
// Reset closes the peer's write end (peerW) rather than this end's read pipe,
// so a blocked Read returns io.EOF the way spdystream's does, not
// io.ErrClosedPipe.
type scriptedStream struct {
	r, w  net.Conn
	peerW net.Conn
	id    uint32
	hdr   http.Header

	closed atomic.Bool
	reset  atomic.Bool
	eof    atomic.Bool
}

func (s *scriptedStream) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if errors.Is(err, io.EOF) {
		s.eof.Store(true)
	}
	return n, err
}

func (s *scriptedStream) Write(p []byte) (int, error) { return s.w.Write(p) }

func (s *scriptedStream) Close() error {
	s.closed.Store(true)
	return s.w.Close()
}

func (s *scriptedStream) Reset() error {
	s.reset.Store(true)
	return errors.Join(s.peerW.Close(), s.w.Close())
}

func (s *scriptedStream) Headers() http.Header { return s.hdr }

func (s *scriptedStream) Identifier() uint32 { return s.id }

// testStreamServer is the pod side of a scriptedStream pair: r sees what the client
// writes, w is what the client reads.
type testStreamServer struct {
	r net.Conn
	w net.Conn
}

// newStreamPair builds a client stream and its scriptable server side.
func newStreamPair(id uint32) (*scriptedStream, *testStreamServer) {
	sw, cr := net.Pipe()
	cw, sr := net.Pipe()
	client := &scriptedStream{r: cr, w: cw, peerW: sw, id: id, hdr: http.Header{}}
	return client, &testStreamServer{r: sr, w: sw}
}

// newSilentStream builds an error stream whose server side is already closed:
// the client reads EOF at once, the shape a successful forward has.
func newSilentStream(id uint32) *scriptedStream {
	sw, cr := net.Pipe()
	_ = sw.Close()
	cw, sr := net.Pipe()
	_ = sr.Close()
	return &scriptedStream{r: cr, w: cw, peerW: sw, id: id, hdr: http.Header{}}
}

func newTestEndpoint() Endpoint {
	return Endpoint{Namespace: "ns", Pod: "pod", PodIP: "10.0.0.7", Port: 5432}
}

// newTestStreamConn is newStreamConn with the concrete type, so a test can
// reach CloseWrite, which net.Conn does not declare.
func newTestStreamConn(ep Endpoint, data, errStream httpstream.Stream, release func()) *streamConn {
	return newStreamConn(ep, data, errStream, release, nil).(*streamConn)
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// makeStreamConnPipe joins two streamConns back to back so anything written to
// one is read from the other, which is what nettest expects of a connection.
func makeStreamConnPipe() (net.Conn, net.Conn, func(), error) {
	// c1 -> c2 is the p1/p2 pipe, c2 -> c1 is the p3/p4 pipe.
	p1, p2 := net.Pipe()
	p3, p4 := net.Pipe()

	d1 := &scriptedStream{r: p4, w: p1, peerW: p3, id: 1, hdr: http.Header{}}
	d2 := &scriptedStream{r: p2, w: p3, peerW: p1, id: 2, hdr: http.Header{}}

	var releases sync.WaitGroup
	releases.Add(2)
	c1 := newTestStreamConn(newTestEndpoint(), d1, newSilentStream(3), releases.Done)
	c2 := newTestStreamConn(newTestEndpoint(), d2, newSilentStream(4), releases.Done)

	stop := func() {
		_ = c1.Close()
		_ = c2.Close()
		releases.Wait()
	}
	return c1, c2, stop, nil
}

func TestStreamConnNettest(t *testing.T) {
	nettest.TestConn(t, makeStreamConnPipe)
}

// TestStreamConnForwardErrorReplacesEOF pins the store-then-close order: the
// pod's data stream ends while the kubelet's message is still in flight, so a
// Read that saw a bare io.EOF would prove the order was reversed. synctest.Wait
// makes that deterministic: with the correct order Read is durably blocked
// when the message arrives, with the reversed order it has already returned.
func TestStreamConnForwardErrorReplacesEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data, dataSrv := newStreamPair(1)
		errStream, errSrv := newStreamPair(2)
		conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

		if err := dataSrv.w.Close(); err != nil {
			t.Fatalf("close data stream: %v", err)
		}

		readErr := make(chan error, 1)
		go func() {
			_, err := conn.Read(make([]byte, 32))
			readErr <- err
		}()
		synctest.Wait()

		if _, err := errSrv.w.Write([]byte("connection refused")); err != nil {
			t.Fatalf("write error stream: %v", err)
		}
		if err := errSrv.w.Close(); err != nil {
			t.Fatalf("close error stream: %v", err)
		}

		err := <-readErr
		var ferr *ForwardError
		if !errors.As(err, &ferr) {
			t.Fatalf("Read error = %v (%T), want *ForwardError", err, err)
		}
		if ferr.Msg != "connection refused" {
			t.Errorf("ForwardError.Msg = %q, want %q", ferr.Msg, "connection refused")
		}
		if ferr.Namespace != "ns" || ferr.Pod != "pod" || ferr.Port != 5432 {
			t.Errorf("ForwardError = %+v, want the endpoint it was opened for", ferr)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

// TestStreamConnTimedOutErrorStreamDiscardsMessage pins the other half of the
// same rule: a message read only because the wait expired is truncated (the
// real Reset ends the Read with io.EOF), so it must not become a ForwardError.
func TestStreamConnTimedOutErrorStreamDiscardsMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data, dataSrv := newStreamPair(1)
		errStream, errSrv := newStreamPair(2)
		conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

		// The kubelet sends a partial message and never closes the stream.
		if _, err := errSrv.w.Write([]byte("partial")); err != nil {
			t.Fatalf("write error stream: %v", err)
		}
		// The pod's data stream ends, so the relay reaches the message wait
		// with only the partial message buffered.
		if err := dataSrv.w.Close(); err != nil {
			t.Fatalf("close data stream: %v", err)
		}

		_, err := conn.Read(make([]byte, 32))
		var ferr *ForwardError
		if errors.As(err, &ferr) {
			t.Fatalf("Read = %v, want no ForwardError from a timed-out read", err)
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read = %v, want io.EOF", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

func TestStreamConnCleanEOF(t *testing.T) {
	data, dataSrv := newStreamPair(1)
	errStream, errSrv := newStreamPair(2)
	conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

	if err := errSrv.w.Close(); err != nil {
		t.Fatalf("close error stream: %v", err)
	}
	if err := dataSrv.w.Close(); err != nil {
		t.Fatalf("close data stream: %v", err)
	}

	n, err := conn.Read(make([]byte, 32))
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read = (%d, %v), want (0, io.EOF)", n, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStreamConnRemoteAddr(t *testing.T) {
	tests := []struct {
		name  string
		podIP string
		want  string
	}{
		{"ipv4", "10.0.0.7", "10.0.0.7:5432"},
		{"ipv6", "fd00::1", "[fd00::1]:5432"},
		// The lookup layer only forwards to an endpoint with a PodIP, so this
		// is not expected; it must not render as a dialable ":5432".
		{"unparseable", "", "kubedial: unknown pod IP for ns/pod:5432"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep := Endpoint{Namespace: "ns", Pod: "pod", PodIP: tt.podIP, Port: 5432}
			data, dataSrv := newStreamPair(1)
			defer func() { _ = dataSrv.w.Close() }()
			conn := newTestStreamConn(ep, data, newSilentStream(2), func() {})
			defer func() {
				if err := conn.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}()

			ra := conn.RemoteAddr()
			if ra.Network() != "tcp" {
				t.Errorf("RemoteAddr().Network() = %q, want %q", ra.Network(), "tcp")
			}
			if got := ra.String(); got != tt.want {
				t.Errorf("RemoteAddr().String() = %q, want %q", got, tt.want)
			}
			la := conn.LocalAddr()
			if la.Network() != "tcp" {
				t.Errorf("LocalAddr().Network() = %q, want %q", la.Network(), "tcp")
			}
			if got := la.String(); got != "127.0.0.1:0" {
				t.Errorf("LocalAddr().String() = %q, want %q", got, "127.0.0.1:0")
			}
		})
	}
}

func TestStreamConnCloseWriteHalfCloses(t *testing.T) {
	data, dataSrv := newStreamPair(1)
	errStream, errSrv := newStreamPair(2)
	if err := errSrv.w.Close(); err != nil {
		t.Fatalf("close error stream: %v", err)
	}
	conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte("hello"))
		writeErr <- err
	}()
	got := make([]byte, 5)
	if _, err := io.ReadFull(dataSrv.r, got); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("client write: %v", err)
	}

	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if n, err := dataSrv.r.Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("server read after CloseWrite = (%d, %v), want (0, io.EOF)", n, err)
	}
	// A Write after CloseWrite fails at once rather than blocking on a relay
	// that has finished.
	if _, err := conn.Write([]byte("more")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after CloseWrite = %v, want net.ErrClosed", err)
	}

	replyErr := make(chan error, 1)
	go func() {
		_, err := dataSrv.w.Write([]byte("bye"))
		replyErr <- err
	}()
	reply := make([]byte, 3)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("client read after CloseWrite: %v", err)
	}
	if err := <-replyErr; err != nil {
		t.Fatalf("server write after CloseWrite: %v", err)
	}
	if string(reply) != "bye" {
		t.Errorf("reply = %q, want %q", reply, "bye")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestStreamConnCloseWriteFlushesPendingWrite covers the case the half-close
// test hides: a Write immediately followed by CloseWrite, with the chunk still
// in the relay, must reach the pod before the FIN.
func TestStreamConnCloseWriteFlushesPendingWrite(t *testing.T) {
	data, dataSrv := newStreamPair(1)
	errStream, errSrv := newStreamPair(2)
	if err := errSrv.w.Close(); err != nil {
		t.Fatalf("close error stream: %v", err)
	}
	conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

	gotCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(dataSrv.r)
		errCh <- err
		gotCh <- got
	}()

	if _, err := conn.Write([]byte("request")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server read: %v", err)
	}
	if got := <-gotCh; string(got) != "request" {
		t.Fatalf("server received %q, want %q", got, "request")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestStreamConnCloseWriteAfterClose pins that a half-close after the full close
// reports net.ErrClosed, as *net.TCPConn does, rather than the half-close result
// cached before the close. copyConnHalf and closeWrite ignore the error, so this
// is only about the net.Conn contract a caller sees.
func TestStreamConnCloseWriteAfterClose(t *testing.T) {
	data, dataSrv := newStreamPair(1)
	errStream, errSrv := newStreamPair(2)
	if err := dataSrv.w.Close(); err != nil {
		t.Fatalf("close data stream: %v", err)
	}
	if err := errSrv.w.Close(); err != nil {
		t.Fatalf("close error stream: %v", err)
	}
	conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := conn.CloseWrite(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("CloseWrite after Close = %v, want net.ErrClosed", err)
	}
}

func TestStreamConnCloseReleasesOnce(t *testing.T) {
	data, dataSrv := newStreamPair(1)
	errStream, errSrv := newStreamPair(2)
	if err := dataSrv.w.Close(); err != nil {
		t.Fatalf("close data stream: %v", err)
	}
	if err := errSrv.w.Close(); err != nil {
		t.Fatalf("close error stream: %v", err)
	}

	var (
		releases        int
		dataReset       bool
		errorStreamDone bool
		sendDone        bool
		recvDone        bool
	)
	var conn *streamConn
	conn = newTestStreamConn(newTestEndpoint(), data, errStream, func() {
		releases++
		dataReset = data.reset.Load()
		errorStreamDone = errStream.eof.Load()
		sendDone = isClosed(conn.sendDone)
		recvDone = isClosed(conn.recvDone)
	})

	// Drain the read side so both streams reach their end before Close.
	buf := make([]byte, 1)
	for {
		if _, err := conn.Read(buf); err != nil {
			break
		}
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if releases != 1 {
		t.Errorf("release called %d times, want 1", releases)
	}
	if !dataReset {
		t.Error("release ran before the data stream was reset")
	}
	if !errorStreamDone {
		t.Error("release ran before the error stream was read to its end")
	}
	if !sendDone || !recvDone {
		t.Errorf("release ran before the copy goroutines finished: sendDone=%v recvDone=%v", sendDone, recvDone)
	}
}

func TestStreamConnSilentErrorStreamBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data, dataSrv := newStreamPair(1)
		// The error stream's server side is never closed.
		errStream, _ := newStreamPair(2)
		conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

		if err := dataSrv.w.Close(); err != nil {
			t.Fatalf("close data stream: %v", err)
		}

		start := time.Now()
		if err := conn.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if elapsed := time.Since(start); elapsed > errorStreamWait+time.Second {
			t.Errorf("Close took %v, want no more than %v", elapsed, errorStreamWait+time.Second)
		}
	})
}

// TestStreamConnCloseFastDoesNotWaitForErrorStream pins the teardown the ready
// probe relies on. A full Close waits out errorStreamWait when the kubelet never
// closes the error stream; a probe that closed that way would delay every
// Listener bind by that wait. closeFast must return at once, with the streams
// reset and the pool reference released, without joining the copies.
func TestStreamConnCloseFastDoesNotWaitForErrorStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data, _ := newStreamPair(1)
		// The error stream's server side is never closed, the case that makes
		// Close wait errorStreamWait.
		errStream, _ := newStreamPair(2)
		releases := 0
		conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() { releases++ })

		start := time.Now()
		conn.closeFast()
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("closeFast took %v, want effectively no wait", elapsed)
		}
		if releases != 1 {
			t.Errorf("release called %d times, want 1", releases)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("Close after closeFast: %v", err)
		}
		if releases != 1 {
			t.Errorf("Close after closeFast released again: %d releases", releases)
		}
	})
}

// TestReadyProbeReturnsOnBudgetNotErrorStream pins the cost readyProbe must not
// pay: it has its own verdict the moment the budget expires, so it must not
// also wait for the kubelet's error stream, which is what a full Close would do.
// synctest makes the bound exact: an eager close would add errorStreamWait, far
// past the budget.
func TestReadyProbeReturnsOnBudgetNotErrorStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data, _ := newStreamPair(1)
		errStream, _ := newStreamPair(2)
		releases := 0
		conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() { releases++ })

		dial := func(context.Context) (net.Conn, error) { return conn, nil }
		start := time.Now()
		if err := readyProbe(t.Context(), dial, time.Second); err != nil {
			t.Fatalf("readyProbe = %v, want nil for a silent forward", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("readyProbe took %v, want about the 1s budget: it waited for the error stream", elapsed)
		}
		if releases != 1 {
			t.Errorf("readyProbe released %d times, want 1", releases)
		}
	})
}

// TestStreamConnStalledPodSilentKubeletBounded pins the combined worst case:
// the pod never reads the data stream (so the drain expires) and the kubelet
// never closes the error stream (so the message wait expires), which is
// dataDrainWait plus errorStreamWait. Referencing both constants means an edit
// to either has to notice this bound.
func TestStreamConnStalledPodSilentKubeletBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data, _ := newStreamPair(1)
		errStream, _ := newStreamPair(2)
		conn := newTestStreamConn(newTestEndpoint(), data, errStream, func() {})

		// Hand the relay a chunk the pod never reads.
		if _, err := conn.Write([]byte("in flight")); err != nil {
			t.Fatalf("Write: %v", err)
		}

		start := time.Now()
		if err := conn.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if want := dataDrainWait + errorStreamWait + time.Second; time.Since(start) > want {
			t.Errorf("Close took %v, want no more than %v", time.Since(start), want)
		}
	})
}

func TestStreamConnCloseNoGoroutineLeak(t *testing.T) {
	base := runtime.NumGoroutine()

	// The pod's data stream is still live when Close runs: this is not a
	// stream that had already ended.
	data, dataSrv := newStreamPair(1)
	conn := newTestStreamConn(newTestEndpoint(), data, newSilentStream(2), func() {})

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(dataSrv.r, buf); err != nil {
		t.Fatalf("server read: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close joins all three goroutines before it returns, so this is checked
	// directly and at once. Polling the process-wide count would pass even if
	// Close skipped a join, because the reset frees the goroutines anyway.
	if !isClosed(conn.sendDone) || !isClosed(conn.recvDone) || !isClosed(conn.errDone) {
		t.Errorf("Close returned with goroutines still running: sendDone=%v recvDone=%v errDone=%v",
			isClosed(conn.sendDone), isClosed(conn.recvDone), isClosed(conn.errDone))
	}
	if got := runtime.NumGoroutine(); got > base {
		t.Errorf("goroutines after Close = %d, want <= %d; a goroutine outlived Close", got, base)
	}
	// The relay half-closed the data stream as it finished, so the pod saw a
	// FIN rather than only the RST from Close's reset.
	if !data.closed.Load() {
		t.Error("the caller -> pod copy ended without half-closing the data stream")
	}
}
