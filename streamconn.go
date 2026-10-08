package kubedial

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/streaming/pkg/httpstream"
)

// errorStreamWait bounds how long shutdown waits for the kubelet's message on
// a port-forward's error stream. A kubelet that has gone away never closes that
// stream, and an unbounded wait would pin the connection, and the goroutine
// reading it, open forever.
const errorStreamWait = 5 * time.Second

// dataDrainWait bounds how long a half-close waits for data a Write already
// accepted to reach the pod before the stream is closed under it. The relay
// runs one chunk ahead of the pipe, so without this wait a half-close that
// follows a Write drops that chunk; the bound keeps a pod that has stopped
// reading from pinning the call forever.
const dataDrainWait = 5 * time.Second

// streamConn adapts one port-forward's data and error streams to net.Conn.
//
// The data stream is relayed through a net.Pipe rather than handed to the
// caller directly. httpstream.Stream has no deadline methods, and clients such
// as pgx set a deadline to bound a handshake; net.Pipe supplies SetDeadline,
// SetReadDeadline and SetWriteDeadline with the semantics net.Conn promises.
type streamConn struct {
	net.Conn // outer: the caller's view of the pipe, carrying the deadlines

	ep        Endpoint
	data      httpstream.Stream
	errStream httpstream.Stream
	inner     net.Conn

	sendDone  chan struct{} // closed when the caller -> pod copy has finished
	recvDone  chan struct{} // closed when the pod -> caller copy has finished
	errResult chan []byte
	errDone   chan struct{}

	// writeClosed is set by CloseWrite so a later Write fails at once instead
	// of feeding a relay that is finishing.
	writeClosed atomic.Bool
	// pending counts bytes a Write has handed to the relay that the relay has
	// not yet written to the data stream. CloseWrite waits for it to reach zero
	// so the pod sees the FIN after every byte a Write reported as sent.
	pending atomic.Int64
	// writeDone is signalled whenever pending reaches zero.
	writeDone chan struct{}
	// dataReset is set once Close resets the data stream, after which a
	// half-close from the relay is moot and spdystream rejects it.
	dataReset atomic.Bool
	// closed is set once Close has run, so a later CloseWrite reports
	// net.ErrClosed rather than the half-close result cached before the close,
	// which is what *net.TCPConn does.
	closed atomic.Bool

	halfCloseOnce sync.Once
	halfCloseErr  error

	closeOnce sync.Once
	closeErr  error
	release   func()

	// onForwardError, when set, runs once as the connection closes if the kubelet
	// wrote a ForwardError. It lets the dialer drop the resolved address's cache
	// entry: a stream that failed after it was established is as much evidence
	// against the pod as a failed open.
	onForwardError func()

	// otherStreamsOpen reports whether the port-forward connection that carried
	// this stream still has another stream open. It is nil for a conn that is
	// not a port-forward stream, such as a direct dial.
	otherStreamsOpen func() bool

	mu      sync.Mutex
	readErr error
}

// newStreamConn wraps one port-forward's data and error streams as a net.Conn.
// release runs exactly once, after both streams are finished with, and
// otherStreamsOpen reports whether this stream's connection still carries a
// sibling stream (nil when there is no connection to ask, as for a direct
// dial).
func newStreamConn(ep Endpoint, data, errStream httpstream.Stream, release func(), otherStreamsOpen func() bool) net.Conn {
	outer, inner := net.Pipe()
	c := &streamConn{
		Conn:             outer,
		ep:               ep,
		data:             data,
		errStream:        errStream,
		inner:            inner,
		errResult:        make(chan []byte, 1),
		errDone:          make(chan struct{}),
		sendDone:         make(chan struct{}),
		recvDone:         make(chan struct{}),
		writeDone:        make(chan struct{}, 1),
		release:          release,
		otherStreamsOpen: otherStreamsOpen,
	}

	// The client writes nothing to the error stream, so close it up front:
	// this is what tells the kubelet, and it matches client-go's
	// handleConnection.
	if err := errStream.Close(); err != nil {
		c.recordErr(err)
	}
	go c.readErrorStream()

	go c.copyToInner()
	go c.copyToData()

	return c
}

// OtherStreamsOpen reports whether the port-forward connection that carried
// this stream still has another stream open. It is meaningful only after this
// stream has released its own reference: with the reference still held, the
// stream itself counts and the caller must test for more than one. A caller
// that sees one stream end cleanly uses it to tell that stream's end from the
// pod's whole forward ending: only the latter leaves no sibling. It is false for
// a conn with no port-forward connection to ask about.
func (c *streamConn) OtherStreamsOpen() bool {
	return c.otherStreamsOpen != nil && c.otherStreamsOpen()
}

// RemoteAddr reports the pod the stream reached, as <podIP>:<port> (an IPv6 pod
// IP renders as [podIP]:<port>). This is a contract, not a log nicety: pgx
// sends its cancel request to the address of the connection it was given, and
// that request has to arrive at the same pod.
func (c *streamConn) RemoteAddr() net.Addr {
	ip := net.ParseIP(c.ep.PodIP)
	if ip == nil {
		// The lookup layer only forwards to an endpoint that has a PodIP, so
		// this is not expected; report it rather than a ":port" that looks
		// dialable.
		return unknownPodAddr{ep: c.ep}
	}
	return &net.TCPAddr{IP: ip, Port: int(c.ep.Port)}
}

// LocalAddr reports a loopback address: nothing local listens, the connection
// is a stream into the cluster.
func (c *streamConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
}

// unknownPodAddr is what RemoteAddr reports for an endpoint whose PodIP did not
// parse: a network and a name, but no address to dial.
type unknownPodAddr struct {
	ep Endpoint
}

func (a unknownPodAddr) Network() string { return "tcp" }

func (a unknownPodAddr) String() string { return "kubedial: unknown pod IP for " + a.ep.String() }

// Read returns the forward error the kubelet wrote on the error stream in place
// of io.EOF, so a caller sees why the connection ended rather than a silent end
// of file.
func (c *streamConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if errors.Is(err, io.EOF) {
		c.mu.Lock()
		if c.readErr != nil {
			err = c.readErr
		}
		c.mu.Unlock()
	}
	//nolint:wrapcheck // io.Reader's callers branch on the delegate's error; wrapping would turn end of file into a failure.
	return n, err
}

// Write sends p to the pod. After CloseWrite it fails at once, as a write to a
// half-closed connection does, rather than feeding a relay that has finished.
func (c *streamConn) Write(p []byte) (int, error) {
	if c.writeClosed.Load() {
		return 0, net.ErrClosed
	}
	// Reserve the bytes before handing them to the relay. CloseWrite must not
	// close the stream between the relay reading them and writing them out, so
	// it waits on this count; reserving first closes the window where it would
	// see nothing pending and close underneath the write.
	c.pending.Add(int64(len(p)))
	if c.writeClosed.Load() {
		// CloseWrite raced this write: give the reservation back and fail, so
		// nothing is handed to a relay that is closing the stream.
		if c.pending.Add(int64(-len(p))) == 0 {
			c.signalWriteDone()
		}
		return 0, net.ErrClosed
	}
	n, err := c.Conn.Write(p)
	if n < len(p) {
		if c.pending.Add(int64(n-len(p))) == 0 {
			c.signalWriteDone()
		}
	}
	//nolint:wrapcheck // io.Writer's callers branch on the delegate's error; wrapping would break their deadline checks.
	return n, err
}

// CloseWrite half-closes the connection: every byte a Write already reported as
// sent reaches the pod first, then the pod sees end of file on the data stream
// while reads from it continue. This is how a protocol signals it has finished
// sending.
func (c *streamConn) CloseWrite() error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	c.writeClosed.Store(true)
	c.drainPending()
	return c.halfCloseData()
}

// drainPending waits, bounded by dataDrainWait, for the relay to hand every
// byte a Write accepted to the data stream.
func (c *streamConn) drainPending() {
	deadline := time.After(dataDrainWait)
	for c.pending.Load() > 0 {
		select {
		case <-c.writeDone:
		case <-deadline:
			return
		}
	}
}

func (c *streamConn) signalWriteDone() {
	select {
	case c.writeDone <- struct{}{}:
	default:
	}
}

// halfCloseData sends the pod a FIN once, whichever path reaches it first. It
// does not reset the stream, so the pod -> caller direction keeps flowing.
func (c *streamConn) halfCloseData() error {
	c.halfCloseOnce.Do(func() {
		c.halfCloseErr = c.data.Close()
	})
	return c.halfCloseErr
}

// Close tears the connection down. It is idempotent and returns the first
// non-nil error seen while resetting the streams, never swallowing it.
//
// It is bounded. The worst case is dataDrainWait plus errorStreamWait, about
// ten seconds, reached when the pod has stopped reading the data stream so the
// drain has to expire and the kubelet never closes the error stream so the
// message wait has to expire. A healthy pod and kubelet return in microseconds.
func (c *streamConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		// Close the caller's end first so a pending Read or Write on it
		// returns at once instead of waiting on the relay.
		if err := c.Conn.Close(); err != nil {
			c.recordErr(err)
		}
		// Let data the caller's Write already accepted reach the pod before
		// the reset below discards it. Closing outer has stopped this copy's
		// reads, so it can only be finishing a write in flight.
		select {
		case <-c.sendDone:
		case <-time.After(dataDrainWait):
		}
		// Reset the data stream before waiting on the error stream, the order
		// client-go's handleConnection uses: it discards unsent data so the
		// error stream's read cannot block behind it, and it unblocks the
		// pod -> caller copy.
		c.dataReset.Store(true)
		if err := c.data.Reset(); err != nil {
			c.recordErr(err)
		}
		<-c.sendDone
		<-c.recvDone
		// recvDone closes only after copyToInner has taken the error-stream
		// result, so errDone is already closing here; the bound that matters
		// lives in awaitForwardError, not in this wait.
		<-c.errDone
		if c.release != nil {
			c.release()
		}
		if c.onForwardError != nil && c.hasForwardError() {
			c.onForwardError()
		}
	})
	return c.closeErr
}

// closeFast tears the connection down without waiting for the pod -> caller
// copy or the kubelet's error stream to finish. It is what readyProbe uses: the
// probe has already read the verdict it needs, and Close's wait for the error
// stream costs about a second on a healthy forward, because a kubelet closes
// that stream about a second after the data stream is reset. That second would
// otherwise be added to every Listener bind that runs a ready check.
//
// It resets both streams, so the copies stop and the pool can keep the warmed
// connection, and it releases the pool reference exactly as Close does.
func (c *streamConn) closeFast() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		// Close the caller's end first so the relay's write into it fails at
		// once; the probe writes nothing, so there is no accepted data to drain.
		if err := c.Conn.Close(); err != nil {
			c.recordErr(err)
		}
		c.dataReset.Store(true)
		if err := c.data.Reset(); err != nil {
			c.recordErr(err)
		}
		if err := c.errStream.Reset(); err != nil {
			c.recordErr(err)
		}
		if c.release != nil {
			c.release()
		}
		if c.onForwardError != nil && c.hasForwardError() {
			c.onForwardError()
		}
	})
}

// invalidateCacheOnForwardError registers f to run once as the connection
// closes if the kubelet wrote a ForwardError. It is set before the connection is
// returned to a caller, so no lock guards the field.
func (c *streamConn) invalidateCacheOnForwardError(f func()) {
	c.onForwardError = f
}

// hasForwardError reports whether the connection ended with the kubelet's
// forward error rather than a clean close.
func (c *streamConn) hasForwardError() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	var fe *ForwardError
	return errors.As(c.readErr, &fe)
}

// copyToInner relays the pod's data to the caller. Once the pod's side ends,
// it hands over the kubelet's error message before closing the inner end: the
// store must happen first, or a Read racing the close would see a bare io.EOF.
func (c *streamConn) copyToInner() {
	defer close(c.recvDone)
	if _, err := io.Copy(c.inner, c.data); err != nil {
		c.storeReadErr(err)
	}
	c.awaitForwardError()
	if err := c.inner.Close(); err != nil {
		c.recordErr(err)
	}
}

// copyToData relays the caller's data to the pod, then half-closes the stream
// so the pod sees a FIN rather than an RST, the order client-go's
// handleConnection uses (its caller -> pod copy defers dataStream.Close()).
func (c *streamConn) copyToData() {
	defer close(c.sendDone)
	buf := make([]byte, 32*1024)
	for {
		n, err := c.inner.Read(buf)
		if n > 0 {
			if _, werr := c.data.Write(buf[:n]); werr != nil {
				c.storeReadErr(werr)
			}
			if c.pending.Add(int64(-n)) == 0 {
				c.signalWriteDone()
			}
		}
		if err != nil {
			break
		}
	}
	// Close may already have reset the stream, and a half-close after a reset
	// is both moot and, on spdystream, an error.
	if !c.dataReset.Load() {
		if err := c.halfCloseData(); err != nil {
			c.storeReadErr(err)
		}
	}
}

// readErrorStream reads the kubelet's message to completion. The result is
// buffered so this goroutine never blocks on a receiver that has already moved
// on.
func (c *streamConn) readErrorStream() {
	defer close(c.errDone)
	msg, err := io.ReadAll(c.errStream)
	if err != nil {
		// A broken error stream is not a forward error; the data stream's
		// own copy carries the failure the caller needs to see.
		msg = nil
	}
	c.errResult <- msg
}

// awaitForwardError blocks for the kubelet's message, bounded by
// errorStreamWait. On expiry it resets the error stream so the reader unblocks,
// then discards whatever arrived: a message read only because the reset ended
// the read is truncated (spdystream's Reset ends a Read with io.EOF and a nil
// error), and a truncated forward error is worse than none.
func (c *streamConn) awaitForwardError() {
	select {
	case msg := <-c.errResult:
		c.storeForwardError(msg)
	case <-time.After(errorStreamWait):
		if err := c.errStream.Reset(); err != nil {
			c.recordErr(err)
		}
		<-c.errResult
	}
}

func (c *streamConn) storeForwardError(msg []byte) {
	if len(msg) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readErr = &ForwardError{
		Namespace: c.ep.Namespace,
		Pod:       c.ep.Pod,
		Port:      c.ep.Port,
		Msg:       string(msg),
	}
}

// storeReadErr keeps err as the reason a Read past the end reports, unless a
// forward error already explains it. Teardown errors are expected once either
// end closes and are not the caller's to see.
func (c *streamConn) storeReadErr(err error) {
	if err == nil || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr == nil {
		c.readErr = err
	}
}

func (c *streamConn) recordErr(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr == nil {
		c.closeErr = err
	}
}
