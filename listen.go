package kubedial

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// ListenOption configures a Listener.
type ListenOption func(*listenConfig)

type listenConfig struct {
	localPort   uint16
	noRedial    bool
	readyCheck  time.Duration
	onDialError func(error)
}

// WithLocalPort binds the listener to a fixed loopback port. Zero, the default,
// lets the kernel pick one, which Addr then reports.
func WithLocalPort(p uint16) ListenOption {
	return func(c *listenConfig) { c.localPort = p }
}

// WithNoRedial stops the listener after the first forward failure rather than
// redialling for the next connection. It is meaningful with ListenEndpoint: a
// forward to a pinned "primary" that silently redials the same pod after a
// failover reaches a standby, so a caller that asked for one pod gets a
// different one without being told. Stopping and letting the caller decide is
// the only answer that cannot do that.
//
// The pinned pod's forward ending is told from one stream ending on a live pod
// (a Postgres cancel request, a server-side terminate) by whether the
// port-forward connection still carries another stream: only the whole forward
// ending leaves none. The check runs after the ending stream has released its
// own reference, so two streams ending at the same instant cannot each see the
// other and both decline to stop. A stream with no port-forward connection to
// ask about, such as a direct dial, leaves none by definition and stops the
// listener the same way.
func WithNoRedial() ListenOption {
	return func(c *listenConfig) { c.noRedial = true }
}

// WithReadyCheck makes Listen open one stream before returning and watch for a
// kubelet forward error on it for up to timeout. An error means the forward is
// refused and Listen fails with ErrNotReady; silence within the timeout means
// the forward is ready, because a kubelet that cannot reach the port reports it
// on the stream's error side rather than by accepting the connection.
func WithReadyCheck(timeout time.Duration) ListenOption {
	return func(c *listenConfig) { c.readyCheck = timeout }
}

// WithOnDialError registers fn to run whenever dialling an upstream for an
// accepted connection fails. The accept goroutine starts fn in a goroutine of
// its own, so a slow fn cannot stall acceptance; Close waits for it to return.
// It is a notification: the redial decision is made by the listener, not fn.
func WithOnDialError(fn func(error)) ListenOption {
	return func(c *listenConfig) { c.onDialError = fn }
}

// Listener forwards a loopback TCP address to a pod port, one upstream stream
// per accepted connection, for tools that can only be handed an address.
//
// Its lifetime is the context passed to Listen: when that ends the listener
// stops with Err() reporting ErrStopped, and every accepted connection is torn
// down with it.
type Listener struct {
	addr string
	ln   net.Listener

	dial        func(context.Context) (net.Conn, error)
	cancel      context.CancelFunc
	noRedial    bool
	onDialError func(error)

	done   chan struct{}
	stopCh chan struct{}

	stopOnce sync.Once
	wg       sync.WaitGroup

	mu       sync.Mutex
	stopping bool
	err      error
	closeErr error
	conns    map[net.Conn]struct{}
}

// Listen accepts loopback connections and forwards each one to addr, resolving
// the address anew for every accepted connection. When ctx ends the listener
// stops at once and Done closes with the reason. A closed underlying Dialer is
// noticed only on the next accepted connection: its dial fails with ErrStopped
// and the listener then stops, so an idle listener over a closed Dialer keeps
// Done open.
func (d *Dialer) Listen(ctx context.Context, addr string, opts ...ListenOption) (*Listener, error) {
	return d.listen(ctx, func(dctx context.Context) (net.Conn, error) {
		return d.DialContext(dctx, "tcp", addr)
	}, opts...)
}

// ListenEndpoint accepts loopback connections and forwards each one to exactly
// ep. It does not resolve, so every accepted connection reaches the same pod;
// combine it with WithNoRedial to stop rather than fail over.
func (d *Dialer) ListenEndpoint(ctx context.Context, ep Endpoint, opts ...ListenOption) (*Listener, error) {
	if d.pool == nil {
		return nil, errors.New("kubedial: ListenEndpoint requires forward mode")
	}
	return d.listen(ctx, func(dctx context.Context) (net.Conn, error) {
		data, errStream, pc, release, err := d.pool.openConn(dctx, ep)
		if err != nil {
			return nil, err
		}
		// pipe asks this only after it has released this stream's own reference,
		// so any reference left belongs to a sibling: refs > 0, not the pre-release
		// refs > 1. dialForward (the Listen path) must build its callback the same
		// way, through pool.streamsOpenAfterRelease, or one path stops early.
		return newStreamConn(ep, data, errStream, release, func() bool {
			return d.pool.streamsOpenAfterRelease(pc)
		}), nil
	}, opts...)
}

// listen is the shared construction path. dial opens one upstream connection
// for one accepted connection.
func (d *Dialer) listen(ctx context.Context, dial func(context.Context) (net.Conn, error), opts ...ListenOption) (*Listener, error) {
	var lc listenConfig
	for _, opt := range opts {
		opt(&lc)
	}

	if lc.readyCheck > 0 {
		if err := readyProbe(ctx, dial, lc.readyCheck); err != nil {
			return nil, err
		}
	}

	// The derived context lets Close abort an in-flight upstream dial; the
	// original context still ends the listener on its own schedule.
	lctx, cancel := context.WithCancel(ctx)
	ln, err := (&net.ListenConfig{}).Listen(lctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(lc.localPort))))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("kubedial: listen on loopback: %w", err)
	}

	l := &Listener{
		addr:        ln.Addr().String(),
		ln:          ln,
		dial:        dial,
		cancel:      cancel,
		noRedial:    lc.noRedial,
		onDialError: lc.onDialError,
		done:        make(chan struct{}),
		stopCh:      make(chan struct{}),
		conns:       map[net.Conn]struct{}{},
	}

	l.wg.Add(1)
	go l.accept(lctx)
	l.wg.Add(1)
	go l.watch(lctx)
	go func() {
		l.wg.Wait()
		close(l.done)
	}()

	return l, nil
}

// Addr reports the loopback address the listener accepts on. It is fixed for
// the listener's life.
func (l *Listener) Addr() string { return l.addr }

// Done is closed once the listener has stopped and every connection it owned
// has been torn down.
func (l *Listener) Done() <-chan struct{} { return l.done }

// Err reports why the listener stopped, and nil while it is still running. The
// context's own error is rendered into the message but never chained, so a
// caller branching on errors.Is(err, context.Canceled) sees false: the listener
// reports its own sentinel.
func (l *Listener) Err() error {
	select {
	case <-l.done:
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.err
	default:
		return nil
	}
}

// Close stops the listener and waits for its accept loop, its connections and
// their copy goroutines to finish. It is safe to call more than once.
func (l *Listener) Close() error {
	l.stop(ErrStopped)
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeErr
}

// watch ends the listener when its context does. It also exits once the
// listener stops for any other reason, so it never outlives Close.
func (l *Listener) watch(ctx context.Context) {
	defer l.wg.Done()
	select {
	case <-ctx.Done():
		//nolint:errorlint // the context error is rendered, never chained: a caller branching on errors.Is(err, context.Canceled) must see false.
		l.stop(fmt.Errorf("%v: %w", ctx.Err(), ErrStopped))
	case <-l.stopCh:
	}
}

// stop begins shutdown exactly once, recording reason as why. It unblocks
// Accept by closing the socket, unblocks the copy goroutines by closing the
// connections they are on, and cancels the derived context so an in-flight
// upstream dial returns.
func (l *Listener) stop(reason error) {
	l.stopOnce.Do(func() {
		l.mu.Lock()
		l.stopping = true
		l.err = reason
		conns := make([]net.Conn, 0, len(l.conns))
		for c := range l.conns {
			conns = append(conns, c)
		}
		l.mu.Unlock()

		l.cancel()
		close(l.stopCh)

		if err := l.ln.Close(); err != nil {
			l.mu.Lock()
			if l.closeErr == nil {
				l.closeErr = err
			}
			l.mu.Unlock()
		}
		for _, c := range conns {
			//nolint:errcheck // shutdown path: no caller is left to receive a per-connection close error.
			c.Close()
		}
	})
}

// acceptedConn is one l.ln.Accept result, handed from the acceptor goroutine to
// the accept loop so the loop can also drain dial results while a dial is in
// flight and a connection is arriving.
type acceptedConn struct {
	conn net.Conn
	err  error
}

// acceptConnections hands l.ln.Accept's results to accept. It ends when the
// listener stops, closing a connection it accepted but could not deliver.
func (l *Listener) acceptConnections(ctx context.Context, out chan<- acceptedConn) {
	defer l.wg.Done()
	for {
		c, err := l.ln.Accept()
		select {
		case out <- acceptedConn{conn: c, err: err}:
		case <-ctx.Done():
			if c != nil {
				//nolint:errcheck // the listener is stopping; the connection is being discarded.
				c.Close()
			}
			return
		}
		if err != nil {
			return
		}
	}
}

// accept serves the loopback socket. It runs the upstream dial on the accepted
// connection's own goroutine, so a dial that stalls holds up neither the accept
// loop nor the other accepted connections; the dial's outcome comes back over a
// channel so WithOnDialError is still run by this goroutine.
func (l *Listener) accept(ctx context.Context) {
	defer l.wg.Done()

	accepted := make(chan acceptedConn)
	l.wg.Add(1)
	go l.acceptConnections(ctx, accepted)
	dialErrors := make(chan error)
	for {
		select {
		case <-ctx.Done():
			return
		case ac := <-accepted:
			if ac.err != nil {
				if l.isStopping() {
					return
				}
				l.stop(fmt.Errorf("kubedial: listener accept: %w", ac.err))
				return
			}
			if !l.track(ac.conn) {
				// stop began between Accept returning and here; the connection is
				// already inside the set stop will close, so there is nothing to
				// hand on.
				//nolint:errcheck // shutdown path: the connection is being discarded.
				ac.conn.Close()
				return
			}
			l.wg.Add(1)
			go l.dialConn(ctx, ac.conn, dialErrors)
		case derr := <-dialErrors:
			if l.isStopping() || ctx.Err() != nil {
				// The dial failed because the listener is shutting down; that is not
				// a dial failure to report, and shutdown is already under way.
				continue
			}
			l.notifyDialError(derr)
			if l.noRedial || errors.Is(derr, ErrStopped) {
				// Either the pinned target is gone, or the Dialer itself is closed
				// so every future dial would fail the same way. Stop rather than
				// fail connections forever with Done never closing.
				l.stop(fmt.Errorf("kubedial: listener: %w", derr))
				return
			}
		}
	}
}

// dialConn opens one accepted connection's upstream on its own goroutine. On
// success it hands the pair to pipe; on failure it reports the error to the
// accept goroutine, which owns the WithOnDialError call and the redial
// decision. The send is abandoned once the listener stops, so it never leaks.
func (l *Listener) dialConn(ctx context.Context, client net.Conn, dialErrors chan<- error) {
	defer l.wg.Done()

	upstream, derr := l.dial(ctx)
	if derr != nil {
		l.untrack(client)
		//nolint:errcheck // the accepted connection is discarded after a failed dial.
		client.Close()
		if l.isStopping() || ctx.Err() != nil {
			return
		}
		select {
		case dialErrors <- derr:
		case <-ctx.Done():
		}
		return
	}
	if !l.track(upstream) {
		l.untrack(client)
		//nolint:errcheck // shutdown path: both connections are being discarded.
		client.Close()
		//nolint:errcheck // shutdown path: both connections are being discarded.
		upstream.Close()
		return
	}

	l.wg.Add(1)
	go l.pipe(client, upstream)
}

// notifyDialError runs the WithOnDialError callback. The accept goroutine
// starts it, but in a goroutine of its own, so a callback that blocks cannot
// stall acceptance; Close joins it.
func (l *Listener) notifyDialError(err error) {
	if l.onDialError == nil {
		return
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.onDialError(err)
	}()
}

// pipe copies one accepted connection to its upstream and back, then closes
// both. It stops the listener after a failed forward when WithNoRedial is set.
//
// A direction that ends cleanly half-closes its destination, so a protocol that
// signals completion with end of file still gets its reply. A direction that
// ends with an error closes both connections at once instead: half-closing would
// make the failure look like an orderly end of stream, and waiting for the peer
// to close would leave a tool that does not react to a half-close holding the
// connection, and this goroutine, open until the listener stops.
func (l *Listener) pipe(client, upstream net.Conn) {
	defer l.wg.Done()
	defer func() {
		l.untrack(client)
		l.untrack(upstream)
		closeBoth(client, upstream)
	}()

	// clientDone is closed once the client -> upstream copy is over, before it
	// half-closes the upstream. The upstream -> client copy reads it to tell the
	// two ways its own copy can end: the upstream ended on its own (the pod went
	// away) or the client disconnected first and the upstream end is only a
	// consequence. It is closed before the half-close so a test, or a client,
	// that waits for the pod to see end of file cannot race it.
	clientDone := make(chan struct{})
	upToClient := make(chan pipeEnd, 1)
	clientToUp := make(chan error, 1)
	go func() {
		err := copyConnHalf(client, upstream)
		upstreamFirst := true
		select {
		case <-clientDone:
			upstreamFirst = false
		default:
		}
		if err != nil {
			closeBoth(client, upstream)
		}
		upToClient <- pipeEnd{err: err, upstreamFirst: upstreamFirst}
	}()
	go func() {
		err := copyConnHalf(upstream, client)
		close(clientDone)
		if err != nil {
			closeBoth(client, upstream)
		} else {
			closeWrite(upstream)
		}
		clientToUp <- err
	}()

	up := <-upToClient
	if l.noRedial {
		switch {
		case up.err != nil:
			// Either the kubelet refused the forward (*ForwardError) or the
			// port-forward connection itself was lost. Both mean the pod the caller
			// asked for is no longer the one a redial would reach, so the listener
			// stops with that reason rather than silently reconnecting.
			l.stop(fmt.Errorf("kubedial: listener: %w", up.err))
		case up.upstreamFirst:
			// The upstream ended cleanly while the client was still connected. On a
			// port-forward that is ambiguous: a graceful failover ends the stream this
			// way, and so does one stream ending on a live pod, such as a Postgres
			// cancel request, which opens a second short-lived stream and has the pod
			// close it. Only the pod's whole forward ending leaves no sibling stream
			// on the connection, so only that stops the listener; otherwise redialling
			// the pinned name would reach whatever pod now owns it.
			//
			// Release this stream's own reference before asking whether a sibling
			// remains: with the check before the release, two streams ending at the
			// same instant each still hold a reference, each sees the other as a
			// sibling, and neither stops. Closing the upstream here is idempotent
			// with the deferred closeBoth below.
			//nolint:errcheck // the reason to stop is the clean end; a close error is not.
			upstream.Close()
			if !upstreamHasSibling(upstream) {
				l.stop(fmt.Errorf("kubedial: listener: pinned forward ended: %w", ErrStopped))
			}
		}
	}
	// Tell the client the upstream ended only after the sibling decision above. A
	// client that reconnects as soon as it reads EOF (a pgx pool does) would
	// otherwise be accepted, dial and take a reference on this same pooled
	// connection before the check ran, look like a sibling, and suppress the stop.
	if up.err == nil {
		closeWrite(client)
	}
	<-clientToUp
}

// upstreamHasSibling reports whether c is a port-forward stream whose
// connection still carries another stream. A conn that is not one reports
// false, so a clean upstream end reads as the end of the forward.
func upstreamHasSibling(c net.Conn) bool {
	alive, ok := c.(interface{ OtherStreamsOpen() bool })
	return ok && alive.OtherStreamsOpen()
}

// pipeEnd is the result of the upstream -> client copy: its read error, and
// whether the upstream ended before the client had finished.
type pipeEnd struct {
	err           error
	upstreamFirst bool
}

// isStopping reports whether shutdown has begun.
func (l *Listener) isStopping() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stopping
}

// track adds c to the set stop will close. It reports false when shutdown has
// already begun, in which case stop will not close c and the caller must.
func (l *Listener) track(c net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopping {
		return false
	}
	l.conns[c] = struct{}{}
	return true
}

// untrack removes c from the shutdown set.
func (l *Listener) untrack(c net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.conns, c)
}

// copyConnHalf copies src to dst until src ends, and reports src's read error.
// A write failure on dst is not reported: dst went away, and the copy in the
// other direction sees that same end from its own read.
func copyConnHalf(dst, src net.Conn) error {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				//nolint:nilerr // dst is gone; the other direction reports that same end, so this copy is not the one to fail.
				return nil
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) || errors.Is(rerr, net.ErrClosed) || errors.Is(rerr, io.ErrClosedPipe) {
				// A clean end is not a failure to report.
				//nolint:nilerr // a read that ended cleanly is not an error to propagate.
				return nil
			}
			//nolint:wrapcheck // the reader's caller branches on the error, exactly as io.Reader's does.
			return rerr
		}
	}
}

// closeWrite half-closes c's write direction when it supports it, so the peer
// sees end of file while the other direction keeps flowing.
func closeWrite(c net.Conn) {
	cw, ok := c.(interface{ CloseWrite() error })
	if !ok {
		return
	}
	//nolint:errcheck // a failed half-close means the peer is already gone; the full close that follows reports anything left.
	cw.CloseWrite()
}

// closeBoth closes both ends of a pipe at once, the teardown a failed direction
// needs. Errors are dropped: the failure that prompted this is already reported,
// and these closes only unblock the opposite copy.
func closeBoth(a, b net.Conn) {
	//nolint:errcheck // see above.
	a.Close()
	//nolint:errcheck // see above.
	b.Close()
}

// readyProbe opens one stream and watches it for up to timeout. A kubelet
// forward error, or a lost port-forward connection, means the forward did not
// become ready; a clean end of stream or silence means it did, whether the pod
// is waiting for the client to speak first or has simply not answered yet.
func readyProbe(ctx context.Context, dial func(context.Context) (net.Conn, error), timeout time.Duration) error {
	conn, err := dial(ctx)
	if err != nil {
		//nolint:errorlint // the dial failure is rendered, not chained: ErrNotReady is the sentinel callers branch on.
		return fmt.Errorf("kubedial: ready check: %v: %w", err, ErrNotReady)
	}

	readErr := make(chan error, 1)
	go func() {
		var buf [1]byte
		for {
			if _, rerr := conn.Read(buf[:]); rerr != nil {
				readErr <- rerr
				return
			}
		}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-readErr:
		closeProbeConn(conn)
		if errors.Is(err, io.EOF) {
			// The pod ended the stream without a message: silence, then close.
			return nil
		}
		// A *ForwardError is the kubelet refusing the forward; any other non-EOF
		// error is the port-forward connection itself being lost during the
		// check. Neither means the forward became ready.
		//nolint:errorlint // the message is rendered, not chained: ErrNotReady is the sentinel callers branch on.
		return fmt.Errorf("kubedial: forward not ready: %v: %w", err, ErrNotReady)
	case <-ctx.Done():
		closeProbeConn(conn)
		<-readErr
		//nolint:errorlint // the context error is rendered, never chained.
		return fmt.Errorf("%v: %w", ctx.Err(), ErrStopped)
	case <-timer.C:
		closeProbeConn(conn)
		<-readErr
		return nil
	}
}

// closeProbeConn tears down a probe connection. A streamConn offers closeFast,
// which releases the stream without waiting for the pod -> caller copy and the
// kubelet's error stream; a healthy kubelet only finishes that stream about a
// second after the data stream is reset, and a full Close would block the
// listener bind on that second. A connection that is not one, such as a direct
// dial, closes normally.
func closeProbeConn(conn net.Conn) {
	if fc, ok := conn.(interface{ closeFast() }); ok {
		fc.closeFast()
		return
	}
	//nolint:errcheck // probe teardown: the probe is being discarded either way.
	conn.Close()
}

// HTTPClient returns an *http.Client whose requests go through the listener.
//
// The client dials Addr() no matter what host a request names, so callers write
// ordinary URLs against the in-cluster service name and do not have to know a
// listener is involved. The rewrite happens in the transport's DialContext, not
// in the request's URL: the URL's Host reaches the server exactly as the caller
// wrote it, and, for https, so does the TLS SNI name, which is what lets a
// certificate issued for the in-cluster service name verify.
//
// Proxy is nil, not inherited: DefaultTransport carries
// Proxy: ProxyFromEnvironment, and a proxy chosen from the request's own URL
// would be spoken *to the pod*, because DialContext below ignores that URL and
// dials the listener regardless. With HTTPS_PROXY set, an https request would
// write a plaintext CONNECT down the tunnel and never perform a TLS handshake,
// so the caller's tlsConfig would be unused. For https, tlsConfig is used as-is;
// nil means the platform roots, never InsecureSkipVerify.
func (l *Listener) HTTPClient(tlsConfig *tls.Config) *http.Client {
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		// Only trips if something in the process replaced http.DefaultTransport
		// with a different type; the client is still functional, just without
		// that replacement's settings.
		defaultTransport = &http.Transport{}
	}

	transport := defaultTransport.Clone()
	// Clone, not assign: net/http mutates a Transport's TLSClientConfig in
	// place, so assigning the caller's pointer would rewrite a struct the
	// caller still owns. Clone is nil-safe, so nil still means platform roots.
	transport.TLSClientConfig = tlsConfig.Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, l.addr)
	}

	return &http.Client{Transport: transport}
}

// Do performs req through the listener, retrying once when a failure is safe to
// repeat.
//
// Every request dials Addr() through the listener, and the listener opens a
// fresh upstream for it, so a retry here is self-contained rather than waiting
// for a reconnect. It happens only for an idempotent method whose body is
// replayable, never for a request that already produced a response, and never
// when the caller's ctx ended: ctx errors are returned so errors.Is reaches
// them. The retry policy, including reporting both attempts' errors, is the one
// Dialer.Do uses, so the listener and the dialer cannot drift apart.
func (l *Listener) Do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	return doRetry(ctx, client, req, "kubedial: listener")
}
