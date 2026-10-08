package kubedial

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	httpstream "k8s.io/streaming/pkg/httpstream"
)

func newHTTPForwardDialer(t *testing.T, c clients, factory streamDialerFactory) *Dialer {
	t.Helper()
	d, err := New(&rest.Config{Host: "https://example.invalid"},
		WithMode(ModeForward),
		withClients(c),
		withStreamDialerFactory(factory),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return d
}

func newHTTPDirectDialer(t *testing.T) *Dialer {
	t.Helper()
	d, err := New(nil, WithMode(ModeDirect))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return d
}

func TestTransportClonesTLSConfig(t *testing.T) {
	d := newHTTPDirectDialer(t)
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"original"}}
	tr := d.Transport(cfg)

	// Mutating the caller's config after the call must not reach the transport.
	cfg.NextProtos = []string{"mutated"}
	cfg.MinVersion = tls.VersionTLS13

	if tr.TLSClientConfig == cfg {
		t.Fatal("Transport stored the caller's tls.Config directly, want a clone")
	}
	if got := tr.TLSClientConfig.MinVersion; got != tls.VersionTLS12 {
		t.Errorf("cloned MinVersion = %d, want %d", got, tls.VersionTLS12)
	}
	if slices.Contains(tr.TLSClientConfig.NextProtos, "mutated") {
		t.Error("mutating the caller's config reached the transport")
	}
}

func TestTransportNeverSetsInsecureSkipVerify(t *testing.T) {
	d := newHTTPDirectDialer(t)
	for i, cfg := range []*tls.Config{nil, {}} {
		tr := d.Transport(cfg)
		if tr.TLSClientConfig != nil && tr.TLSClientConfig.InsecureSkipVerify {
			t.Errorf("case %d: Transport set InsecureSkipVerify on a caller config", i)
		}
	}

	// A caller that supplies it keeps it: kubedial neither sets nor clears it.
	tr := d.Transport(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // the point of the case is the caller's value surviving.
	if tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
		t.Error("caller's InsecureSkipVerify was not preserved")
	}
}

// webServiceClientsTLS is webServiceClients with the Service on 443, so an
// https URL with no explicit port exercises the rule's https default port.
func webServiceClientsTLS(podIP string) *recordingClients {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "https", Port: 443}}
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{{Name: strPtr("https"), Port: int32Ptr(8443)}},
		endpoint("web-1", podIP, nil),
	)
	return &recordingClients{inner: newFakeClients(svc, slice)}
}

// proxyEnv is http.ProxyFromEnvironment without net/http's process-wide cache:
// net/http reads the proxy environment once per process on first use, so a test
// that asserted against the cached function would depend on whether an earlier
// test had already populated it. Reading the variable here keeps each proxy test
// independent of test order.
func proxyEnv(req *http.Request) (*url.URL, error) {
	name := "HTTP_PROXY"
	if req.URL.Scheme == "https" {
		name = "HTTPS_PROXY"
	}
	raw := os.Getenv(name)
	if raw == "" {
		return nil, nil
	}
	return url.Parse(raw)
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

func TestTransportProxyNilForInClusterHost(t *testing.T) {
	// The rule is about https: the scheme picks the default port Resolve sees, so
	// this uses an https URL and a Service on 443. HTTPS_PROXY is set so that, if
	// the in-cluster branch were skipped, the environment proxy would return a
	// non-nil URL and fail this test.
	t.Setenv("HTTPS_PROXY", "http://proxy.example:3128")

	d := newHTTPForwardDialer(t, webServiceClientsTLS("10.1.0.5"), newFakeTransport().factory)
	proxy := d.proxyFuncWith(proxyEnv)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://web.ns.svc/health", nil)
	got, err := proxy(req)
	if err != nil {
		t.Fatalf("Proxy: %v", err)
	}
	if got != nil {
		t.Errorf("Proxy(in-cluster https host) = %v, want nil", got)
	}
}

func TestTransportProxyFromEnvForExternalHost(t *testing.T) {
	// net/http caches the proxy environment process-wide on first use, so this
	// test must not depend on another test's ordering: it asserts against
	// proxyEnv, an uncached read of HTTPS_PROXY, and calls the proxy function
	// directly rather than relying on a live round trip.
	t.Setenv("HTTPS_PROXY", "http://proxy.example:3128")

	rec := &recordingClients{inner: newFakeClients()}
	d := newHTTPForwardDialer(t, rec, newFakeTransport().factory)
	proxy := d.proxyFuncWith(proxyEnv)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/x", nil)
	got, err := proxy(req)
	if err != nil {
		t.Fatalf("Proxy: %v", err)
	}
	want := mustParseURL(t, "http://proxy.example:3128")
	if got == nil || got.String() != want.String() {
		t.Errorf("Proxy(external https host) = %v, want %v", got, want)
	}
	// The external host must have gone through Resolve before reaching the
	// environment, or the environment result proves nothing about the rule.
	if got := rec.callCount(); got == 0 {
		t.Error("Proxy made no cluster call, so it did not consult Resolve")
	}
}

// TestTransportWiresDialerProxy pins that Transport installs the dialer's own
// Proxy rule instead of leaving the cloned default ProxyFromEnvironment in
// place. A forward dialer's rule is a distinct closure, so comparing code
// pointers distinguishes them without consulting the environment (and its
// process-wide cache) at all.
func TestTransportWiresDialerProxy(t *testing.T) {
	d := newHTTPForwardDialer(t, webServiceClients("10.1.0.5"), newFakeTransport().factory)
	tr := d.Transport(nil)
	if reflect.ValueOf(tr.Proxy).Pointer() == reflect.ValueOf(http.ProxyFromEnvironment).Pointer() {
		t.Error("Transport left the default ProxyFromEnvironment, want the dialer's per-host rule")
	}
}

func TestTransportDirectModeUsesEnvProxy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://proxy.example:3128")

	d := newHTTPDirectDialer(t)
	proxy := d.proxyFuncWith(proxyEnv)

	// The same in-cluster name that forward mode skips must honour the
	// environment in direct mode, because direct mode dials every address as
	// written.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://web.ns.svc/health", nil)
	got, err := proxy(req)
	if err != nil {
		t.Fatalf("Proxy: %v", err)
	}
	want := mustParseURL(t, "http://proxy.example:3128")
	if got == nil || got.String() != want.String() {
		t.Errorf("Proxy(direct mode) = %v, want %v", got, want)
	}
}

// countingEnvProxy is an injected environment proxy that records whether it was
// consulted, so a test can prove a path never reached it.
type countingEnvProxy struct {
	url   *url.URL
	mu    sync.Mutex
	calls int
}

func (p *countingEnvProxy) proxy(*http.Request) (*url.URL, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return p.url, nil
}

func (p *countingEnvProxy) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func TestTransportProxyReturnsResolveError(t *testing.T) {
	// A Resolve error names an in-cluster host that could not be looked up, here
	// a qualified Service that does not exist. The proxy must return that error
	// so the request fails with the real reason; falling through to the
	// environment would tunnel the internal hostname to the corporate proxy.
	rec := &recordingClients{inner: newFakeClients()}
	d := newHTTPForwardDialer(t, rec, newFakeTransport().factory)

	env := &countingEnvProxy{url: mustParseURL(t, "http://proxy.example:3128")}
	tr := d.Transport(nil)
	tr.Proxy = d.proxyFuncWith(env.proxy)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://missing.ns.svc/x", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if resp != nil {
		_ = resp.Body.Close()
		t.Fatalf("response = %v, want nil", resp)
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Do error = %v, want errors.Is(..., ErrNotFound)", err)
	}
	if got := env.callCount(); got != 0 {
		t.Errorf("environment proxy consulted %d times on a Resolve error, want 0", got)
	}
}

// tcpStream is an httpstream.Stream backed by one net.Conn, so a port-forward in
// a test carries real bytes to an httptest.Server.
type tcpStream struct {
	net.Conn
	hdr http.Header
}

func (s *tcpStream) Reset() error         { return s.Close() }
func (s *tcpStream) Headers() http.Header { return s.hdr }
func (s *tcpStream) Identifier() uint32   { return 0 }

// serverDialer hands out connections whose data streams dial addr, the address
// of an httptest.Server, so a request made through the Dialer reaches a real
// server over the fake port-forward.
type serverDialer struct {
	ctx  context.Context
	addr string
}

func (d *serverDialer) Dial(_ ...string) (httpstream.Connection, string, error) {
	return &serverConn{ctx: d.ctx, addr: d.addr, closeCh: make(chan bool)}, portforward.PortForwardProtocolV1Name, nil
}

type serverConn struct {
	ctx     context.Context
	addr    string
	closeCh chan bool
	once    sync.Once
	id      atomic.Uint32
}

func (c *serverConn) CreateStream(h http.Header) (httpstream.Stream, error) {
	if h.Get(corev1.StreamType) == corev1.StreamTypeError {
		return newSilentStream(c.id.Add(1)), nil
	}
	var d net.Dialer
	conn, err := d.DialContext(c.ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	return &tcpStream{Conn: conn, hdr: h}, nil
}

func (c *serverConn) Close() error {
	c.once.Do(func() { close(c.closeCh) })
	return nil
}

func (c *serverConn) CloseChan() <-chan bool             { return c.closeCh }
func (c *serverConn) SetIdleTimeout(time.Duration)       {}
func (c *serverConn) RemoveStreams(...httpstream.Stream) {}

func serverFactory(ctx context.Context, addr string) streamDialerFactory {
	return func(_, _ string) (httpstream.Dialer, error) {
		return &serverDialer{ctx: ctx, addr: addr}, nil
	}
}

func TestHTTPClientReachesInClusterService(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Host; got != "web.ns.svc:80" {
			t.Errorf("server saw Host %q, want %q", got, "web.ns.svc:80")
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	d := newHTTPForwardDialer(t, webServiceClients("10.1.0.5"), serverFactory(t.Context(), srv.Listener.Addr().String()))
	client := d.HTTPClient(nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://web.ns.svc:80/health", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}
}

// countingRT fails the first failFirst attempts, so a test can count exactly how
// many times Do sent the request.
type countingRT struct {
	mu        sync.Mutex
	attempts  int
	failFirst int
}

func (rt *countingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.attempts++
	if rt.attempts <= rt.failFirst {
		return nil, errors.New("fake: dial failed")
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
}

func (rt *countingRT) count() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.attempts
}

func TestDoRetriesIdempotentOnce(t *testing.T) {
	d := newHTTPDirectDialer(t)
	rt := &countingRT{failFirst: 1}
	client := &http.Client{Transport: rt}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://svc.ns.svc:80/x", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := d.Do(t.Context(), client, req)
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

// bodyRecordingRT reads each attempt's body and fails the first attempt, so a
// test can prove the retry sends the body again rather than reusing the drained
// reader.
type bodyRecordingRT struct {
	mu       sync.Mutex
	bodies   [][]byte
	failOnce bool
}

func (rt *bodyRecordingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if err := req.Body.Close(); err != nil {
			return nil, err
		}
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.bodies = append(rt.bodies, body)
	if len(rt.bodies) == 1 && rt.failOnce {
		return nil, errors.New("fake: first attempt failed")
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
}

func (rt *bodyRecordingRT) bodyAt(i int) []byte {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if i >= len(rt.bodies) {
		return nil
	}
	return rt.bodies[i]
}

func TestDoRetrySendsBodyAgain(t *testing.T) {
	d := newHTTPDirectDialer(t)
	rt := &bodyRecordingRT{failOnce: true}
	client := &http.Client{Transport: rt}

	// bytes.Reader makes http.NewRequest set GetBody, so the body is replayable.
	const payload = "hello world"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://svc.ns.svc:80/x", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	if req.GetBody == nil {
		t.Fatal("test setup: GetBody is nil, so the body is not replayable")
	}

	resp, err := d.Do(t.Context(), client, req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if got := string(rt.bodyAt(1)); got != payload {
		t.Errorf("second attempt read body %q, want %q; the retry reused the drained body", got, payload)
	}
}

// twoErrRT fails with a distinct error on each attempt, so a test can prove a
// failed retry reports both.
type twoErrRT struct {
	mu       sync.Mutex
	attempts int
	first    error
	second   error
}

func (rt *twoErrRT) RoundTrip(*http.Request) (*http.Response, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.attempts++
	if rt.attempts == 1 {
		return nil, rt.first
	}
	return nil, rt.second
}

func TestDoFailedRetryReportsBothErrors(t *testing.T) {
	d := newHTTPDirectDialer(t)
	first := errors.New("fake: first attempt failed")
	second := errors.New("fake: retry failed")
	rt := &twoErrRT{first: first, second: second}
	client := &http.Client{Transport: rt}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://svc.ns.svc:80/x", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := d.Do(t.Context(), client, req)
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

func TestDoNeverRetriesPost(t *testing.T) {
	d := newHTTPDirectDialer(t)
	rt := &countingRT{failFirst: 100}
	client := &http.Client{Transport: rt}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://svc.ns.svc:80/x", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := d.Do(t.Context(), client, req)
	if err == nil {
		t.Fatal("Do(POST) = nil error, want the transport failure")
	}
	if resp != nil {
		defer resp.Body.Close()
		t.Errorf("Do(POST) response = %v, want nil", resp)
	}
	if got := rt.count(); got != 1 {
		t.Errorf("attempts = %d, want 1 (POST is never retried)", got)
	}
}

func TestDoNeverRetriesUnreplayableBody(t *testing.T) {
	d := newHTTPDirectDialer(t)
	rt := &countingRT{failFirst: 100}
	client := &http.Client{Transport: rt}

	// A body built from an arbitrary io.Reader leaves GetBody nil, so it cannot
	// be replayed even though GET itself is retryable.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://svc.ns.svc:80/x", plainReader{strings.NewReader("payload")})
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	if req.GetBody != nil {
		t.Fatal("test setup: GetBody is set, so the body is replayable")
	}

	resp, err := d.Do(t.Context(), client, req)
	if err == nil {
		t.Fatal("Do = nil error, want the transport failure")
	}
	if resp != nil {
		defer resp.Body.Close()
		t.Errorf("Do response = %v, want nil", resp)
	}
	if got := rt.count(); got != 1 {
		t.Errorf("attempts = %d, want 1 (an unreplayable body is never retried)", got)
	}
}

// plainReader hides the concrete reader type so http.NewRequest cannot set
// GetBody.
type plainReader struct{ io.Reader }

// redirectRT answers every request with a redirect. With a CheckRedirect that
// returns an error, client.Do reports a non-nil response together with that
// error: the one case where a response and a failure arrive together, and the
// shape Do must not retry.
type redirectRT struct {
	attempts atomic.Int64
}

func (rt *redirectRT) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.attempts.Add(1)
	return &http.Response{
		StatusCode: http.StatusFound,
		Header:     http.Header{"Location": []string{"http://svc.ns.svc:80/next"}},
		Body:       http.NoBody,
		Request:    req,
	}, nil
}

func TestDoNeverRetriesAfterResponse(t *testing.T) {
	d := newHTTPDirectDialer(t)
	rt := &redirectRT{}
	client := &http.Client{
		Transport: rt,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("fake: redirect refused")
		},
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://svc.ns.svc:80/x", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := d.Do(t.Context(), client, req)
	if err == nil {
		t.Fatal("Do = nil error, want the redirect failure")
	}
	if resp == nil {
		t.Fatal("Do dropped a non-nil response")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusFound)
	}
	if got := rt.attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (a completed round trip is never retried)", got)
	}
}

// ctxErrRT surfaces a wrapped context.Canceled, the way net/http reports a
// cancelled request.
type ctxErrRT struct {
	mu       sync.Mutex
	attempts int
}

func (rt *ctxErrRT) RoundTrip(*http.Request) (*http.Response, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.attempts++
	return nil, fmt.Errorf("fake transport: %w", context.Canceled)
}

func TestDoContextErrorsReachable(t *testing.T) {
	d := newHTTPDirectDialer(t)
	rt := &ctxErrRT{}
	client := &http.Client{Transport: rt}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://svc.ns.svc:80/x", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := d.Do(t.Context(), client, req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do error = %v, want errors.Is(..., context.Canceled)", err)
	}
	if resp != nil {
		defer resp.Body.Close()
		t.Errorf("Do response = %v, want nil", resp)
	}
	if rt.attempts != 1 {
		t.Errorf("attempts = %d, want 1 (a cancelled context is never retried)", rt.attempts)
	}
}

// cancelErrRT cancels the caller's context during the attempt and fails with a
// plain, non-context error. That leaves Do with an ended ctx and an error that
// is not a context sentinel, which is exactly the case its ctx.Err() gate exists
// for: the first two guards do not catch it, so only the gate prevents a retry.
type cancelErrRT struct {
	cancel   context.CancelFunc
	attempts atomic.Int64
}

func (rt *cancelErrRT) RoundTrip(*http.Request) (*http.Response, error) {
	rt.attempts.Add(1)
	rt.cancel()
	return nil, errors.New("fake: transport failed")
}

func TestDoDoesNotRetryWhenContextEnded(t *testing.T) {
	d := newHTTPDirectDialer(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	rt := &cancelErrRT{cancel: cancel}
	client := &http.Client{Transport: rt}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://svc.ns.svc:80/x", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := d.Do(ctx, client, req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do error = %v, want errors.Is(..., context.Canceled)", err)
	}
	if resp != nil {
		defer resp.Body.Close()
		t.Errorf("Do response = %v, want nil", resp)
	}
	if got := rt.attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (a request whose ctx ended is never retried)", got)
	}
}
