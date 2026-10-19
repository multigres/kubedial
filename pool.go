package kubedial

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	//nolint:staticcheck // client-go's spdy.Upgrader is still declared on this package's Connection, so implementing it needs the name.
	apihttpstream "k8s.io/apimachinery/pkg/util/httpstream"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	httpstream "k8s.io/streaming/pkg/httpstream"
)

// streamDialerFactory returns the httpstream.Dialer for one pod's portforward
// URL. It is a function rather than an interface so a caller can build the
// per-pod URL through its own typed client without the pool knowing the client.
type streamDialerFactory func(namespace, pod string) (httpstream.Dialer, error)

// pool holds at most one portforward connection per pod and opens stream pairs
// on it. One connection per pod is what makes a pool worth having: a fresh SPDY
// upgrade costs a TCP and TLS handshake and a round trip, and a process that
// makes many short requests to the same pod would otherwise pay it every time.
type pool struct {
	factory streamDialerFactory
	idle    time.Duration

	// mu guards conns, live, closed and nextSeq, and the poolConn fields refs,
	// requestID, idleTimer, evicted and closed. It is never held across a dial,
	// a CreateStream or a Close: those touch the network and must not stall
	// unrelated opens.
	mu      sync.Mutex
	conns   map[string]*poolConn
	live    map[*poolConn]struct{}
	closed  bool
	nextSeq uint64

	// wg tracks the per-connection CloseChan watchers so Close can join them.
	wg sync.WaitGroup
}

// poolConn is one pod's connection and the bookkeeping around its lifetime.
//
// ready is closed once, under pool.mu, when the dial that created the entry
// finishes. conn and dialErr are written under pool.mu before ready closes, so
// a reader that has observed ready may read them without the lock. refs,
// requestID, idleTimer, evicted and closed are guarded by pool.mu throughout;
// key and seq are immutable after creation.
type poolConn struct {
	key string
	seq uint64

	ready   chan struct{}
	conn    httpstream.Connection
	dialErr error

	refs      int
	requestID uint32
	idleTimer *time.Timer
	idleGen   uint64
	evicted   bool
	closed    bool
}

// newPool returns a pool that dials through factory and closes an unused
// connection idle after its last stream is released.
func newPool(factory streamDialerFactory, idle time.Duration) *pool {
	return &pool{
		factory: factory,
		idle:    idle,
		conns:   map[string]*poolConn{},
		live:    map[*poolConn]struct{}{},
	}
}

// open returns a data and an error stream to ep, and a release func that must
// be called once when the caller is done with both.
func (p *pool) open(ctx context.Context, ep Endpoint) (data, errStream httpstream.Stream, release func(), err error) {
	data, errStream, _, release, err = p.openConn(ctx, ep)
	return data, errStream, release, err
}

// openConn is open, and additionally reports the connection the streams were
// opened on so a caller can ask whether it still carries other streams.
func (p *pool) openConn(ctx context.Context, ep Endpoint) (data, errStream httpstream.Stream, pc *poolConn, release func(), err error) {
	pc, requestID, err := p.acquire(ctx, ep)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	data, errStream, err = p.createStreams(pc, ep, requestID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var once sync.Once
	return data, errStream, pc, func() { once.Do(func() { p.release(pc, data, errStream) }) }, nil
}

// streamsOpenAfterRelease reports whether pc still carries a stream once the
// asking stream's own reference has been released. pipe releases the ending
// stream before it asks, so any reference left is a sibling's and refs > 0 is
// the whole test.
//
// Both listener paths must ask this the same way: dialForward (the Listen path)
// and ListenEndpoint build their streamConn callbacks from it. If one of them
// instead used the pre-release refs > 1, a clean end with exactly one sibling
// left would read as "no sibling" and stop a listener that should keep running.
func (p *pool) streamsOpenAfterRelease(pc *poolConn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return pc.refs > 0
}

// acquire returns the connection for ep with one reference taken, dialling it
// if the pod has none. The caller must release the reference.
func (p *pool) acquire(ctx context.Context, ep Endpoint) (*poolConn, uint32, error) {
	key := ep.Namespace + "/" + ep.Pod

	for {
		// A cancellation check before the select below, because select has no
		// arm priority: without it a cancelled context would still take a
		// reference on a connection that happens to be ready right now.
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, 0, ErrStopped
		}

		pc, ok := p.conns[key]
		switch {
		case !ok:
			pc = &poolConn{key: key, seq: p.nextSeq, ready: make(chan struct{})}
			p.nextSeq++
			p.conns[key] = pc
			p.mu.Unlock()
			// Dial in a goroutine the WaitGroup does not track, so neither this
			// caller nor pool.Close waits out the establish timeout. Then wait on
			// this entry's ready with a ctx.Done() arm, exactly as a caller that
			// arrives while the dial is in flight does; looping instead would race
			// a fast failure and start a second dial for the same caller.
			go func() {
				//nolint:errcheck // the outcome reaches callers through pc.ready and pc.dialErr.
				p.dial(pc, ep)
			}()
			if err := waitDial(ctx, pc); err != nil {
				return nil, 0, err
			}
			continue

		case pc.conn == nil:
			// A dial for this pod is in flight. Wait for it and take its outcome,
			// success or failure, as this caller's own: the callers waiting on one
			// dial share its result. A caller that arrives after the failure finds
			// no entry and dials fresh.
			p.mu.Unlock()
			if err := waitDial(ctx, pc); err != nil {
				return nil, 0, err
			}
			continue

		default:
			pc.refs++
			if pc.idleTimer != nil {
				pc.idleTimer.Stop()
				pc.idleTimer = nil
			}
			requestID := pc.requestID
			pc.requestID++
			p.mu.Unlock()
			return pc, requestID, nil
		}
	}
}

// waitDial blocks until pc's dial has finished or ctx ends, and returns the
// dial's recorded error. It must be called without p.mu held.
func waitDial(ctx context.Context, pc *poolConn) error {
	select {
	case <-pc.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	return pc.dialErr
}

// dial opens the connection for pc and registers it, or records the failure for
// the waiters sharing this dial to receive. It is called by exactly one
// goroutine per entry, without p.mu held.
func (p *pool) dial(pc *poolConn, ep Endpoint) error {
	dialer, err := p.factory(ep.Namespace, ep.Pod)
	if err != nil {
		p.failDial(pc, err)
		return err
	}

	conn, _, err := dialer.Dial(portforward.PortForwardProtocolV1Name)
	if err != nil {
		err = fmt.Errorf("dial port-forward to %s: %w", pc.key, err)
		p.failDial(pc, err)
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.failDial(pc, ErrStopped)
		//nolint:errcheck // the pool is shutting down and has no caller left to report to.
		conn.Close()
		return ErrStopped
	}
	pc.conn = conn
	p.live[pc] = struct{}{}
	close(pc.ready)
	p.wg.Add(1)
	go p.watchClose(pc)
	// A connection with no streams yet still has to be reaped if nobody uses
	// it, so the idle clock starts the moment the dial succeeds.
	p.armIdleLocked(pc)
	p.mu.Unlock()
	return nil
}

// failDial records a failed dial and unlinks the entry so a later open dials
// again. Waiters wake on ready and receive this dial's error; the entry is gone,
// so the error is not cached past its own dial group.
func (p *pool) failDial(pc *poolConn, err error) {
	p.mu.Lock()
	if cur, ok := p.conns[pc.key]; ok && cur == pc {
		delete(p.conns, pc.key)
	}
	pc.dialErr = err
	close(pc.ready)
	p.mu.Unlock()
}

// watchClose evicts a connection as soon as the peer closes it, so the next
// open for that pod dials a fresh one.
func (p *pool) watchClose(pc *poolConn) {
	defer p.wg.Done()
	<-pc.conn.CloseChan()

	p.mu.Lock()
	doClose := p.evictLocked(pc)
	p.mu.Unlock()
	if doClose {
		//nolint:errcheck // the peer already closed it; there is no caller to report to.
		pc.conn.Close()
	}
}

// createStreams opens the error stream and then the data stream for one
// request, in the order client-go does. A failure evicts the connection so the
// next open redials.
func (p *pool) createStreams(pc *poolConn, ep Endpoint, requestID uint32) (data, errStream httpstream.Stream, err error) {
	id := strconv.FormatUint(uint64(requestID), 10)
	port := strconv.FormatInt(int64(ep.Port), 10)

	errStream, err = pc.conn.CreateStream(streamHeaders(corev1.StreamTypeError, port, id))
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("create error stream: %w", err), p.fail(pc))
	}

	data, err = pc.conn.CreateStream(streamHeaders(corev1.StreamTypeData, port, id))
	if err != nil {
		// The data stream never existed, so the error stream would otherwise
		// be left on the connection with nothing reading it.
		var resetErr error
		if rerr := errStream.Reset(); rerr != nil {
			resetErr = fmt.Errorf("reset error stream: %w", rerr)
		}
		pc.conn.RemoveStreams(errStream)
		return nil, nil, errors.Join(fmt.Errorf("create data stream: %w", err), resetErr, p.fail(pc))
	}
	return data, errStream, nil
}

// streamHeaders builds the headers for one port-forward stream.
func streamHeaders(streamType, port, requestID string) http.Header {
	h := http.Header{}
	h.Set(corev1.StreamType, streamType)
	h.Set(corev1.PortHeader, port)
	h.Set(corev1.PortForwardRequestIDHeader, requestID)
	return h
}

// release drops the reference the caller took in acquire and forgets the two
// streams. When the last reference goes, the connection either starts its idle
// clock or, if it was evicted, closes at once.
//
// release must be called exactly once per open, and by the time the pool is
// closed both streams must have been reset or fully closed: pool.Close joins
// each connection's CloseChan watcher, and spdystream's shutdown waits for every
// stream still in its map before it closes the socket.
func (p *pool) release(pc *poolConn, data, errStream httpstream.Stream) {
	pc.conn.RemoveStreams(data, errStream)

	p.mu.Lock()
	if pc.refs > 0 {
		pc.refs--
	}
	var doClose bool
	if pc.refs == 0 {
		if pc.evicted {
			doClose = p.closeLocked(pc)
		} else {
			p.armIdleLocked(pc)
		}
	}
	p.mu.Unlock()

	if doClose {
		//nolint:errcheck // no caller is left to receive it.
		pc.conn.Close()
	}
}

// fail drops the reference of an open whose stream creation failed and evicts
// the connection. Sibling streams keep it alive until they are released. When
// the failed open held the last reference it returns the close error, which the
// caller joins into its own.
func (p *pool) fail(pc *poolConn) error {
	p.mu.Lock()
	if pc.refs > 0 {
		pc.refs--
	}
	doClose := p.evictLocked(pc)
	p.mu.Unlock()

	if !doClose {
		return nil
	}
	if err := pc.conn.Close(); err != nil {
		return fmt.Errorf("close port-forward connection: %w", err)
	}
	return nil
}

// evictLocked unlinks pc so the next open dials again. It does not close a
// connection that still has streams on it: that happens when the last one is
// released. It reports whether the caller must close pc.conn once the lock is
// released.
func (p *pool) evictLocked(pc *poolConn) bool {
	pc.evicted = true
	if cur, ok := p.conns[pc.key]; ok && cur == pc {
		delete(p.conns, pc.key)
	}
	if pc.refs == 0 {
		return p.closeLocked(pc)
	}
	return false
}

// closeLocked marks pc closed, stops its idle timer and unlinks it from both
// maps. It reports whether the caller must call pc.conn.Close() once the lock
// is released. Idempotent.
func (p *pool) closeLocked(pc *poolConn) bool {
	if pc.closed {
		return false
	}
	pc.closed = true
	if pc.idleTimer != nil {
		pc.idleTimer.Stop()
		pc.idleTimer = nil
	}
	delete(p.live, pc)
	if cur, ok := p.conns[pc.key]; ok && cur == pc {
		delete(p.conns, pc.key)
	}
	return true
}

// armIdleLocked starts the idle clock on a connection with no streams. It
// counts streams, never bytes: an idle pgx connection sends no bytes for
// minutes and has to survive. For the same reason SetIdleTimeout is never used.
func (p *pool) armIdleLocked(pc *poolConn) {
	if pc.closed || pc.evicted || p.closed {
		return
	}
	if pc.idleTimer != nil {
		pc.idleTimer.Stop()
	}
	// The generation, not the timer pointer, identifies this timer inside its
	// callback: a closure over a pointer assigned after AfterFunc returns would
	// race the callback against that assignment.
	pc.idleGen++
	gen := pc.idleGen
	pc.idleTimer = time.AfterFunc(p.idle, func() { p.idleExpire(pc, gen) })
}

// idleExpire closes a connection whose idle clock ran out with no streams on
// it. It acts only for the generation currently armed: a callback that fired
// just before an open stopped and replaced its timer must not close the
// connection that open went on to re-arm.
func (p *pool) idleExpire(pc *poolConn, gen uint64) {
	p.mu.Lock()
	if pc.closed || pc.refs != 0 || pc.idleGen != gen {
		p.mu.Unlock()
		return
	}
	doClose := p.closeLocked(pc)
	p.mu.Unlock()

	if doClose {
		//nolint:errcheck // no caller is left to receive it.
		pc.conn.Close()
	}
}

// Close closes every connection, stops every idle timer and joins the
// CloseChan watchers. An open after Close returns ErrStopped. Close is
// idempotent.
func (p *pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true

	pcs := make([]*poolConn, 0, len(p.live))
	for pc := range p.live {
		pcs = append(pcs, pc)
	}
	// p.live is a map, so order the closes before joining their errors: the
	// returned text must not vary with map iteration order.
	sort.Slice(pcs, func(i, j int) bool {
		if pcs[i].key != pcs[j].key {
			return pcs[i].key < pcs[j].key
		}
		return pcs[i].seq < pcs[j].seq
	})
	conns := make([]httpstream.Connection, 0, len(pcs))
	for _, pc := range pcs {
		if p.closeLocked(pc) {
			conns = append(conns, pc.conn)
		}
	}
	p.mu.Unlock()

	errs := make([]error, 0, len(conns))
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close port-forward connection: %w", err))
		}
	}
	p.wg.Wait()
	return errors.Join(errs...)
}

// newRESTStreamDialerFactory returns a factory that builds one pod's dialer
// from cfg and core, mirroring kubectl's createDialer. The SPDY transport and
// upgrader are built once here, so a bad rest.Config fails at construction
// rather than on every dial.
func newRESTStreamDialerFactory(cfg *rest.Config, core corev1client.CoreV1Interface, establishTimeout time.Duration) (streamDialerFactory, error) {
	if establishTimeout <= 0 {
		return nil, fmt.Errorf("establish timeout must be positive, got %s", establishTimeout)
	}

	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("build spdy transport: %w", err)
	}
	wrapped := &establishDeadlineRoundTripper{next: transport, timeout: establishTimeout}

	return func(namespace, pod string) (httpstream.Dialer, error) {
		url := core.RESTClient().Post().
			Resource("pods").
			Namespace(namespace).
			Name(pod).
			SubResource("portforward").
			URL()

		spdyDialer := spdy.NewDialerForStreaming(unauthorizedUpgrader{next: upgrader}, &http.Client{Transport: wrapped}, http.MethodPost, url)

		// The websocket dialer never reads rest.Config.Dial: client-go builds a
		// gorilla dialer and reaches the network through the request context,
		// which the tunneling dialer sets to context.Background(). Wrapping the
		// transport is the only way the deadline reaches that context, and it
		// bounds the whole handshake rather than just the TCP connect.
		cfgCopy := rest.CopyConfig(cfg)
		cfgCopy.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			return &establishDeadlineRoundTripper{next: rt, timeout: establishTimeout}
		})
		wsDialer, err := portforward.NewSPDYOverWebsocketDialerForStreaming(url, cfgCopy)
		if err != nil {
			return nil, fmt.Errorf("build websocket dialer: %w", err)
		}

		// A 401 needs no classifying on the websocket side: client-go reports
		// any non-101 handshake there as an UpgradeFailureError, which falls
		// back to SPDY, so the error a caller sees for a 401 is always the
		// SPDY dialer's.
		return portforward.NewFallbackDialerForStreaming(wsDialer, spdyDialer, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		}), nil
	}, nil
}

// unauthorizedUpgrader marks a 401 on the SPDY upgrade as ErrUnauthorized.
// SpdyRoundTripper.NewConnection renders every non-101 response into a plain
// string error that drops the status code, so the response it is handed is
// the last place a 401 can be told apart from any other refusal.
type unauthorizedUpgrader struct {
	next spdy.Upgrader
}

func (u unauthorizedUpgrader) NewConnection(resp *http.Response) (apihttpstream.Connection, error) {
	conn, err := u.next.NewConnection(resp)
	if err != nil {
		if resp.StatusCode == http.StatusUnauthorized {
			return nil, &unauthorizedError{err: err}
		}
		//nolint:wrapcheck // an Upgrader returns its delegate's error as is; Negotiate is the boundary that reports it.
		return nil, err
	}
	return conn, nil
}
