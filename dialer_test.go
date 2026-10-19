package kubedial

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// recordingClients wraps the lookup fakes and counts every method call, so a
// test can prove a path never touched the cluster clients.
type recordingClients struct {
	inner *fakeClients

	mu    sync.Mutex
	calls int
}

func (r *recordingClients) bump() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
}

func (r *recordingClients) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingClients) Services(ns string) ServiceGetter {
	r.bump()
	return r.inner.Services(ns)
}

func (r *recordingClients) Pods(ns string) PodLister {
	r.bump()
	return r.inner.Pods(ns)
}

func (r *recordingClients) EndpointSlices(ns string) EndpointSliceLister {
	r.bump()
	return r.inner.EndpointSlices(ns)
}

// webServiceClients returns clients where web.ns:80 maps to a pod at podIP.
func webServiceClients(podIP string) *recordingClients {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
		endpoint("web-1", podIP, nil),
	)
	return &recordingClients{inner: newFakeClients(svc, slice)}
}

// twoEndpointClients returns clients where web.ns:80 maps to two ready pods, so
// a resolve has a spread to choose from.
func twoEndpointClients() *recordingClients {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
		endpoint("web-1", "10.1.0.1", nil),
		endpoint("web-2", "10.1.0.2", nil),
	)
	return &recordingClients{inner: newFakeClients(svc, slice)}
}

// gatedServiceGetter blocks in Get until release is closed, so a test can hold a
// resolve inside lookup and invalidate the address while it is in flight.
type gatedServiceGetter struct {
	inner   ServiceGetter
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (g gatedServiceGetter) Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Service, error) {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.inner.Get(ctx, name, opts)
}

type gatedClients struct {
	inner   clients
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedClients) Services(ns string) ServiceGetter {
	return gatedServiceGetter{inner: g.inner.Services(ns), entered: g.entered, release: g.release, once: &g.once}
}

func (g *gatedClients) Pods(ns string) PodLister { return g.inner.Pods(ns) }

func (g *gatedClients) EndpointSlices(ns string) EndpointSliceLister {
	return g.inner.EndpointSlices(ns)
}

func newForwardDialer(t *testing.T, c clients, f *fakeTransport) *Dialer {
	t.Helper()
	d, err := New(&rest.Config{Host: "https://example.invalid"},
		WithMode(ModeForward),
		withClients(c),
		withStreamDialerFactory(f.factory),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestNewModeAutoInCluster(t *testing.T) {
	d, err := New(nil, withInClusterProbe(func() bool { return true }))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := d.Mode(); got != ModeDirect {
		t.Errorf("Mode = %v, want %v", got, ModeDirect)
	}
}

func TestNewModeAutoOutOfCluster(t *testing.T) {
	d, err := New(&rest.Config{Host: "https://example.invalid"},
		withInClusterProbe(func() bool { return false }),
		withClients(&recordingClients{inner: newFakeClients()}),
		withStreamDialerFactory(newFakeTransport().factory),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := d.Mode(); got != ModeForward {
		t.Errorf("Mode = %v, want %v", got, ModeForward)
	}
}

func TestNewForwardRequiresConfig(t *testing.T) {
	// The probe is injected even though an explicit mode never runs it, so a
	// reordering of New cannot make this test read the filesystem.
	if _, err := New(nil, WithMode(ModeForward), withInClusterProbe(func() bool { return false })); err == nil {
		t.Fatal("New with no config in forward mode = nil error, want an error")
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	cases := []struct {
		name string
		opt  Option
	}{
		{"empty cluster domain", WithClusterDomain("")},
		{"zero establish timeout", WithEstablishTimeout(0)},
		{"negative establish timeout", WithEstablishTimeout(-time.Second)},
		{"unknown mode", WithMode(Mode(99))},
		{"zero pod idle", WithPodIdle(0)},
		{"negative pod idle", WithPodIdle(-time.Second)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The probe is injected so that, if validate ever moved after the
			// mode decision, this test would not read the filesystem or env. It
			// settles to direct mode, where a nil config is valid, so the error
			// can only come from the option under test.
			if _, err := New(nil, withInClusterProbe(func() bool { return true }), tc.opt); err == nil {
				t.Fatalf("New with %s = nil error, want an error", tc.name)
			}
		})
	}
}

// TestNewWithPodIdleBoundsPooledConnection checks WithPodIdle reaches the pool:
// a pooled connection outlives the 2m default and closes only once the
// configured idle has passed with no stream on it.
func TestNewWithPodIdleBoundsPooledConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = 10 * time.Minute
		f := newFakeTransport()
		d, err := New(&rest.Config{Host: "https://example.invalid"},
			WithMode(ModeForward),
			WithPodIdle(idle),
			withClients(webServiceClients("10.1.0.1")),
			withStreamDialerFactory(f.factory),
		)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer d.Close()

		conn, err := d.DialContext(t.Context(), "tcp", "web.ns.svc:80")
		if err != nil {
			t.Fatalf("DialContext: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		pooled := f.connAt(0)

		time.Sleep(defaultConfig().podIdle + time.Second)
		if pooled.isClosed() {
			t.Fatal("connection closed at the default idle, want it held for the configured idle")
		}
		time.Sleep(idle - defaultConfig().podIdle)
		if !pooled.isClosed() {
			t.Error("connection not closed after the configured idle with no streams")
		}
	})
}

func TestDialerDirectNeverResolves(t *testing.T) {
	rec := &recordingClients{inner: newFakeClients()}
	d, err := New(nil, WithMode(ModeDirect), withClients(rec))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ln, err := listenLoopback(t.Context())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	conn, err := d.DialContext(t.Context(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer conn.Close()

	// Resolve is still callable in direct mode, for diagnostics, but it reports
	// ok=false without consulting the cluster.
	if _, ok, err := d.Resolve(t.Context(), "web.ns.svc:80"); err != nil {
		t.Fatalf("Resolve: %v", err)
	} else if ok {
		t.Error("Resolve = ok=true in direct mode, want false")
	}

	if got := rec.callCount(); got != 0 {
		t.Errorf("client calls = %d, want 0; direct mode must not resolve", got)
	}
}

func TestDialerExternalDialsDirect(t *testing.T) {
	rec := &recordingClients{inner: newFakeClients()}
	f := newFakeTransport()
	d := newForwardDialer(t, rec, f)

	ln, err := listenLoopback(t.Context())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// 127.0.0.1 is a loopback IP, so Resolve must report ok=false and the dial
	// must reach the local listener without touching the cluster clients. If the
	// loopback rule is removed, the IP goes through lookup instead: that costs a
	// cluster client call, which fails the count assertion below.
	conn, err := d.DialContext(t.Context(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("DialContext(%s): %v", ln.Addr(), err)
	}
	defer conn.Close()

	if got := rec.callCount(); got != 0 {
		t.Errorf("client calls = %d, want 0 for a loopback address", got)
	}
	if got := f.dialCount(); got != 0 {
		t.Errorf("port-forward dials = %d, want 0 for a loopback address", got)
	}
}

func TestDialerUnsupportedNetwork(t *testing.T) {
	rec := webServiceClients("10.1.0.5")
	f := newFakeTransport()
	d := newForwardDialer(t, rec, f)

	_, err := d.DialContext(t.Context(), "udp", "web.ns.svc:80")
	if !errors.Is(err, ErrUnsupportedNetwork) {
		t.Fatalf("DialContext udp = %v, want ErrUnsupportedNetwork", err)
	}
	if got := f.dialCount(); got != 0 {
		t.Errorf("port-forward dials = %d, want 0 when the network is refused", got)
	}
}

func TestDialerOpenErrorInvalidatesCache(t *testing.T) {
	const addr = "web.ns.svc:80"
	rec := webServiceClients("10.1.0.5")
	f := newFakeTransport()
	f.err = errors.New("fake: dial failed")
	d := newForwardDialer(t, rec, f)

	if _, err := d.DialContext(t.Context(), "tcp", addr); err == nil {
		t.Fatal("DialContext = nil error, want a stream-open failure")
	}
	if _, _, found := d.cache.get(addr); found {
		t.Error("cache still holds the address after a stream-open failure")
	}

	// The entry is gone, so the next dial must resolve again. The client call
	// count is what proves it: the pool dials afresh after any failure whether
	// or not the entry was invalidated, so a dial count alone would pass even
	// with the invalidation deleted.
	before := rec.callCount()
	if _, err := d.DialContext(t.Context(), "tcp", addr); err == nil {
		t.Fatal("second DialContext = nil error, want a stream-open failure")
	}
	if rec.callCount() == before {
		t.Error("second dial made no client call; the cache entry was not invalidated")
	}
}

func TestDialerOpenCancelDoesNotInvalidateCache(t *testing.T) {
	const addr = "web.ns.svc:80"
	rec := webServiceClients("10.1.0.5")
	f := newFakeTransport()
	d := newForwardDialer(t, rec, f)

	// A successful dial primes the cache with a good entry.
	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	conn.Close()

	// A caller whose own context ends is not evidence against the address, so
	// the entry must survive the failed open.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := d.DialContext(ctx, "tcp", addr); !errors.Is(err, context.Canceled) {
		t.Fatalf("DialContext with a cancelled context = %v, want context.Canceled", err)
	}
	if _, _, found := d.cache.get(addr); !found {
		t.Error("a cancelled dial invalidated a good cache entry")
	}
}

func TestDialerRemoteAddrIsPodIP(t *testing.T) {
	rec := webServiceClients("10.1.0.5")
	f := newFakeTransport()
	d := newForwardDialer(t, rec, f)

	conn, err := d.DialContext(t.Context(), "tcp", "web.ns.svc:80")
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer conn.Close()

	if got, want := conn.RemoteAddr().String(), "10.1.0.5:8080"; got != want {
		t.Errorf("RemoteAddr = %q, want %q", got, want)
	}
}

// TestDialerStreamForwardErrorInvalidatesCache pins F2b: a kubelet ForwardError
// on an established stream is evidence against the pod, so the address's cache
// entry must be dropped when the stream ends, not only when opening it failed.
func TestDialerStreamForwardErrorInvalidatesCache(t *testing.T) {
	const addr = "web.ns.svc:80"
	rec := webServiceClients("10.1.0.5")
	fc := newFakeConn()
	fc.errMsg = []byte("error dialing backend: connection refused")
	f := newFakeTransport()
	f.queue = []*fakeConn{fc}
	d := newForwardDialer(t, rec, f)

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	if _, _, found := d.cache.get(addr); !found {
		t.Fatal("cache entry missing right after a successful dial")
	}

	// The forward error surfaces only as the stream ends.
	conn.Close()
	if _, _, found := d.cache.get(addr); found {
		t.Error("cache still holds the address after the stream ended with a ForwardError")
	}

	before := rec.callCount()
	if _, _, err := d.Resolve(t.Context(), addr); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if rec.callCount() == before {
		t.Error("re-resolve made no client call; the entry was not invalidated")
	}
}

// TestDialerRemoteAddrDialHitsCache pins F7: resolution also keys the endpoint by
// the address RemoteAddr reports, so a redial there (pgx's cancel request) is a
// cache hit and reaches the same pod without a cluster call even when the
// endpoint's family is not the one status.podIP names.
func TestDialerRemoteAddrDialHitsCache(t *testing.T) {
	const addr = "web.ns.svc:80"
	rec := webServiceClients("10.1.0.5")
	f := newFakeTransport()
	d := newForwardDialer(t, rec, f)
	// If the RemoteAddr cache write is missing, the redial below falls through to
	// a direct dial of 10.1.0.5:8080. Refuse that dial locally so the mutation
	// fails at once and offline instead of making a real connection attempt.
	d.direct = refusingDialer()

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	remote := conn.RemoteAddr().String()
	conn.Close()

	before := rec.callCount()
	c2, err := d.DialContext(t.Context(), "tcp", remote)
	if err != nil {
		t.Fatalf("DialContext(%s): %v", remote, err)
	}
	defer c2.Close()
	if got := rec.callCount(); got != before {
		t.Errorf("redial to %s made %d client calls, want 0 (cache hit)", remote, got-before)
	}
	if got := c2.RemoteAddr().String(); got != remote {
		t.Errorf("RemoteAddr = %q, want %q", got, remote)
	}
}

// refusingDialer is a net.Dialer that fails every dial before it reaches the
// network, so a test that must not dial out fails promptly and offline rather
// than hanging on a real connection attempt.
func refusingDialer() net.Dialer {
	return net.Dialer{
		ControlContext: func(context.Context, string, string, syscall.RawConn) error {
			return errors.New("test: direct dial refused")
		},
	}
}

// TestResolveGenerationDropsStalePut pins the per-key generation guard: a lookup
// that starts before an invalidate and lands after it must not put the dead
// endpoint back, or the address resolves to a dead pod for the positive TTL.
// The RemoteAddr key is written from the same lookup and has its own generation,
// so it is covered too: the derived write must drop when the address write does.
func TestResolveGenerationDropsStalePut(t *testing.T) {
	const addr = "web.ns.svc:80"
	const remoteAddr = "10.1.0.5:8080"
	rec := webServiceClients("10.1.0.5")
	g := &gatedClients{inner: rec, entered: make(chan struct{}), release: make(chan struct{})}
	d := newForwardDialer(t, g, newFakeTransport())

	done := make(chan struct{})
	go func() {
		defer close(done)
		//nolint:errcheck // the outcome is asserted through the cache below.
		d.Resolve(t.Context(), addr)
	}()
	<-g.entered // the lookup is held inside the gated Service Get

	// A dial's open failure invalidates the address while the lookup is in flight.
	d.cache.invalidate(addr)
	close(g.release)
	<-done

	if _, _, found := d.cache.get(addr); found {
		t.Error("cache holds an entry put by a lookup that was invalidated mid-flight")
	}
	if _, _, found := d.cache.get(remoteAddr); found {
		t.Error("cache holds the RemoteAddr entry put by a lookup that was invalidated mid-flight")
	}
}

// TestResolveSpreadsAcrossEndpoints pins the cache-layer spreading: the pick
// among ready candidates is cached for the TTL, and a later resolve after an
// invalidation can land on another replica.
func TestResolveSpreadsAcrossEndpoints(t *testing.T) {
	const addr = "web.ns.svc:80"
	rec := twoEndpointClients()
	d := newForwardDialer(t, rec, newFakeTransport())

	var picks int
	d.pick = func(n int) int {
		picks++
		return picks - 1 // first call -> 0, second -> 1
	}

	first, ok, err := d.Resolve(t.Context(), addr)
	if err != nil || !ok {
		t.Fatalf("first Resolve: ok=%v err=%v", ok, err)
	}
	if first.Pod != "web-1" {
		t.Fatalf("first pod = %q, want web-1", first.Pod)
	}

	// Within the TTL the cached pick is reused and the picker is not consulted.
	again, _, err := d.Resolve(t.Context(), addr)
	if err != nil {
		t.Fatalf("second Resolve: %v", err)
	}
	if again != first {
		t.Fatalf("second resolve = %+v, want the cached %+v", again, first)
	}
	if picks != 1 {
		t.Fatalf("picker called %d times, want 1 (cached pick reused)", picks)
	}

	// After an invalidation a new resolve picks again and can land elsewhere.
	d.cache.invalidate(addr)
	third, ok, err := d.Resolve(t.Context(), addr)
	if err != nil || !ok {
		t.Fatalf("third Resolve: ok=%v err=%v", ok, err)
	}
	if third.Pod != "web-2" {
		t.Fatalf("third pod = %q, want web-2 (spread to the other replica)", third.Pod)
	}
	if picks != 2 {
		t.Fatalf("picker called %d times, want 2", picks)
	}
}

// TestResolveSingleCandidateSkipsPicker pins that a single candidate is taken as
// is, with no picker call: the pod-IP and headless-pod kinds have exactly one.
func TestResolveSingleCandidateSkipsPicker(t *testing.T) {
	const addr = "web.ns.svc:80"
	rec := webServiceClients("10.1.0.5")
	d := newForwardDialer(t, rec, newFakeTransport())
	d.pick = func(int) int {
		t.Error("picker called for a single candidate")
		return 0
	}

	if _, ok, err := d.Resolve(t.Context(), addr); err != nil || !ok {
		t.Fatalf("Resolve: ok=%v err=%v", ok, err)
	}
}

func TestLookupHostInClusterUnchanged(t *testing.T) {
	rec := webServiceClients("10.1.0.5")
	d := newForwardDialer(t, rec, newFakeTransport())

	cases := []string{
		"web.ns.svc",     // an existing Service, resolved via lookup
		"db-0.db.ns.svc", // a headless pod name
		"10.1.0.5",       // a pod IP literal
	}
	for _, host := range cases {
		got, err := d.LookupHost(t.Context(), host)
		if err != nil {
			t.Errorf("LookupHost(%q): %v", host, err)
			continue
		}
		if len(got) != 1 || got[0] != host {
			t.Errorf("LookupHost(%q) = %v, want [%q]", host, got, host)
		}
	}
}

func TestLookupHostExternalUsesDNS(t *testing.T) {
	rec := &recordingClients{inner: newFakeClients()}
	d := newForwardDialer(t, rec, newFakeTransport())

	res := &fakeResolver{addrs: []string{"93.184.216.34"}}
	d.resolver = res

	got, err := d.LookupHost(t.Context(), "example.com")
	if err != nil {
		t.Fatalf("LookupHost: %v", err)
	}
	if len(got) != 1 || got[0] != "93.184.216.34" {
		t.Errorf("LookupHost = %v, want [93.184.216.34]", got)
	}
	if calls := res.called(); len(calls) != 1 || calls[0] != "example.com" {
		t.Errorf("resolver calls = %v, want [example.com]", calls)
	}
}

func TestLookupHostQualifiedMissingErrors(t *testing.T) {
	rec := &recordingClients{inner: newFakeClients()}
	d := newForwardDialer(t, rec, newFakeTransport())
	res := &fakeResolver{addrs: []string{"93.184.216.34"}}
	d.resolver = res

	// A qualified name asserts in-cluster intent, so a missing Service is an
	// error rather than a silent fall-through to DNS, and the resolver is never
	// consulted.
	if _, err := d.LookupHost(t.Context(), "missing.ns.svc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupHost(missing.ns.svc) = %v, want ErrNotFound", err)
	}
	if calls := res.called(); len(calls) != 0 {
		t.Errorf("resolver calls = %v, want none for a qualified missing name", calls)
	}
}

func TestLookupHostInClusterServiceVariants(t *testing.T) {
	check := func(t *testing.T, d *Dialer, host string) {
		t.Helper()
		res := &fakeResolver{addrs: []string{"203.0.113.9"}}
		d.resolver = res

		got, err := d.LookupHost(t.Context(), host)
		if err != nil {
			t.Fatalf("LookupHost(%q): %v", host, err)
		}
		if len(got) != 1 || got[0] != host {
			t.Errorf("LookupHost(%q) = %v, want [%q]", host, got, host)
		}
		if calls := res.called(); len(calls) != 0 {
			t.Errorf("resolver calls = %v, want none for in-cluster %q", calls, host)
		}
	}

	t.Run("headless service is ok", func(t *testing.T) {
		svc := newService("ns", "pg", corev1.ClusterIPNone)
		slice := newSlice("ns", "pg",
			[]discoveryv1.EndpointPort{{Name: strPtr("pg"), Port: int32Ptr(5432)}},
			endpoint("pg-0", "10.1.0.7", nil),
		)
		d := newForwardDialer(t, &recordingClients{inner: newFakeClients(svc, slice)}, newFakeTransport())
		check(t, d, "pg.ns.svc")
	})

	t.Run("external name service exists but is not forwardable", func(t *testing.T) {
		svc := newService("ns", "ext", "")
		svc.Spec.Type = corev1.ServiceTypeExternalName
		svc.Spec.ExternalName = "db.example.com"
		d := newForwardDialer(t, &recordingClients{inner: newFakeClients(svc)}, newFakeTransport())
		check(t, d, "ext.ns.svc")
	})
}

func TestLookupHostCachesServiceVerdict(t *testing.T) {
	rec := webServiceClients("10.1.0.5")
	d := newForwardDialer(t, rec, newFakeTransport())

	now := time.Now()
	d.cache.now = func() time.Time { return now }

	if _, err := d.LookupHost(t.Context(), "web.ns.svc"); err != nil {
		t.Fatalf("first LookupHost: %v", err)
	}
	first := rec.callCount()
	if first == 0 {
		t.Fatal("first LookupHost made no client calls; nothing was resolved")
	}

	if _, err := d.LookupHost(t.Context(), "web.ns.svc"); err != nil {
		t.Fatalf("second LookupHost: %v", err)
	}
	if got := rec.callCount(); got != first {
		t.Errorf("second LookupHost made %d client calls, want 0 (cached verdict)", got-first)
	}

	// The verdict is a positive entry, so it expires with the positive TTL.
	now = now.Add(d.cache.positive + time.Second)
	if _, err := d.LookupHost(t.Context(), "web.ns.svc"); err != nil {
		t.Fatalf("LookupHost after TTL: %v", err)
	}
	if got := rec.callCount(); got == first {
		t.Error("LookupHost made no client call after the positive TTL expired")
	}
}

// TestLookupHostCachesNegativeVerdict pins R5: an unqualified two-label name
// that is not an in-cluster Service has its negative verdict cached, so the next
// LookupHost goes straight to DNS without another Service Get. The DNS answer
// itself is not cached: only the "not a Service" verdict is.
func TestLookupHostCachesNegativeVerdict(t *testing.T) {
	rec := &recordingClients{inner: newFakeClients()}
	d := newForwardDialer(t, rec, newFakeTransport())
	res := &fakeResolver{addrs: []string{"93.184.216.34"}}
	d.resolver = res

	got, err := d.LookupHost(t.Context(), "example.com")
	if err != nil {
		t.Fatalf("first LookupHost: %v", err)
	}
	if len(got) != 1 || got[0] != "93.184.216.34" {
		t.Fatalf("first LookupHost = %v, want the DNS answer", got)
	}
	first := rec.callCount()
	if first == 0 {
		t.Fatal("first LookupHost made no client calls; nothing was looked up")
	}

	got, err = d.LookupHost(t.Context(), "example.com")
	if err != nil {
		t.Fatalf("second LookupHost: %v", err)
	}
	if len(got) != 1 || got[0] != "93.184.216.34" {
		t.Errorf("second LookupHost = %v, want the DNS answer", got)
	}
	if second := rec.callCount(); second != first {
		t.Errorf("second LookupHost made %d client calls, want 0 (cached negative verdict)", second-first)
	}
	if calls := res.called(); len(calls) != 2 {
		t.Errorf("resolver calls = %v, want two: the DNS answer is not cached", calls)
	}
}

func TestCacheErrorsNotCached(t *testing.T) {
	const addr = "missing.ns.svc:80"
	rec := &recordingClients{inner: newFakeClients()}
	f := newFakeTransport()
	d := newForwardDialer(t, rec, f)

	if _, _, err := d.Resolve(t.Context(), addr); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resolve = %v, want ErrNotFound", err)
	}
	if _, _, found := d.cache.get(addr); found {
		t.Fatal("cache holds an entry for an address whose lookup errored")
	}

	before := rec.callCount()
	if _, _, err := d.Resolve(t.Context(), addr); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Resolve = %v, want ErrNotFound", err)
	}
	if rec.callCount() == before {
		t.Error("second Resolve made no client call; the error was cached")
	}
}

func TestDialerCloseStops(t *testing.T) {
	rec := &recordingClients{inner: newFakeClients()}
	f := newFakeTransport()
	d := newForwardDialer(t, rec, f)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}

	if _, err := d.DialContext(t.Context(), "tcp", "web.ns.svc:80"); !errors.Is(err, ErrStopped) {
		t.Errorf("DialContext after Close = %v, want ErrStopped", err)
	}
}

// fakeResolver records the hosts a Dialer asks it to resolve.
type fakeResolver struct {
	mu    sync.Mutex
	hosts []string
	addrs []string
}

func (r *fakeResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hosts = append(r.hosts, host)
	return r.addrs, nil
}

func (r *fakeResolver) called() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.hosts...)
}

// listenLoopback opens a listener the dial tests can reach without a network
// call. It goes through ListenConfig rather than net.Listen so the dial carries
// the test's context, which the linter requires.
func listenLoopback(ctx context.Context) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", "127.0.0.1:0")
}
