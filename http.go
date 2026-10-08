package kubedial

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
)

// retryableMethods is every HTTP method this layer will send twice. POST and
// PATCH are absent: a caller that wants those retried has to say so itself,
// because silently repeating one is how one rollout becomes two. That is a
// promise about this layer, not about the wire: net/http's own Transport
// replays a POST or PATCH carrying an Idempotency-Key or X-Idempotency-Key
// header regardless of this map, since the caller who sets one of those
// headers has already made the opt-in this map exists to require.
var retryableMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
	http.MethodPut:     true,
	http.MethodDelete:  true,
}

// Transport returns an *http.Transport whose requests are dialled through the
// Dialer.
//
// Routing is per host: DialContext resolves each request's own address, so one
// transport reaches every in-cluster Service as well as external hosts. The
// URL's host still reaches the server as the caller wrote it, and, for https,
// so does the TLS SNI name, which is what lets a certificate issued for the
// in-cluster Service name verify.
//
// tlsConfig is cloned, not assigned: net/http mutates a Transport's
// TLSClientConfig in place the first time it is used (http2configureTransports
// appends to NextProtos), so assigning the caller's pointer directly would
// rewrite a struct the caller still owns. Clone is nil-safe, so nil still means
// platform roots. This package never sets InsecureSkipVerify; a caller that
// needs it supplies a config that already carries it.
func (d *Dialer) Transport(tlsConfig *tls.Config) *http.Transport {
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		// http.DefaultTransport is documented as *http.Transport; this only
		// trips if something in the process replaced it with a different type,
		// such as an otelhttp-wrapped one. The transport this builds is still
		// functional, just missing that replacement's settings (HTTP/2,
		// handshake and idle timeouts) rather than "nothing sensible to clone
		// from."
		defaultTransport = &http.Transport{}
	}

	transport := defaultTransport.Clone()
	transport.TLSClientConfig = tlsConfig.Clone()
	transport.DialContext = d.DialContext
	transport.Proxy = d.proxyFunc()
	return transport
}

// HTTPClient returns an *http.Client that reaches in-cluster addresses through
// the Dialer and external addresses directly. Its transport is Transport, so
// the cloning and proxy rules there apply.
func (d *Dialer) HTTPClient(tlsConfig *tls.Config) *http.Client {
	return &http.Client{Transport: d.Transport(tlsConfig)}
}

// proxyFunc is the transport's Proxy rule, deferring to the process environment
// for hosts that are not in-cluster.
func (d *Dialer) proxyFunc() func(*http.Request) (*url.URL, error) {
	return d.proxyFuncWith(http.ProxyFromEnvironment)
}

// proxyFuncWith is proxyFunc with the environment proxy injected, so a test can
// exercise the classification without net/http's process-wide environment
// cache.
//
// In direct mode every address is dialled exactly as written, so the
// environment decides, as it would for a plain net/http client. In forward mode
// a host Resolve claims is reached through a stream and must skip the proxy: a
// proxy chosen from the request's own URL would be spoken to the pod, because
// DialContext ignores that URL and opens a stream regardless. With HTTPS_PROXY
// set, an https request would then write a plaintext CONNECT down the tunnel
// and never perform a TLS handshake, so the caller's tlsConfig would go unused
// and the failure would arrive as a bare EOF. NO_PROXY cannot fix that from
// outside, because the host it matches on is the in-cluster name and never the
// pod address actually dialled. Every other host keeps honouring the
// environment, so external addresses still respect HTTPS_PROXY.
func (d *Dialer) proxyFuncWith(envProxy func(*http.Request) (*url.URL, error)) func(*http.Request) (*url.URL, error) {
	if d.mode == ModeDirect {
		return envProxy
	}
	return func(req *http.Request) (*url.URL, error) {
		host := req.URL.Host
		if req.URL.Port() == "" {
			port := "80"
			if req.URL.Scheme == "https" {
				port = "443"
			}
			host = net.JoinHostPort(req.URL.Hostname(), port)
		}
		// A Resolve error names something in-cluster that could not be looked
		// up: a missing Service, a Service with no ready endpoint, a wrong port,
		// or an API blip. Returning the error fails the request with that
		// reason; falling through to the environment proxy instead would hand
		// the in-cluster hostname to a proxy that cannot resolve it and leak the
		// name off the machine.
		_, ok, err := d.Resolve(req.Context(), host)
		if err != nil {
			return nil, err
		}
		if ok {
			return nil, nil
		}
		return envProxy(req)
	}
}

// Do performs req through client, retrying once when the first attempt failed
// before any response arrived and the request is safe to send twice.
//
// A request is retried only when it is idempotent (GET HEAD OPTIONS TRACE PUT
// DELETE) and its body, if any, can be rebuilt from scratch. Everything else,
// including a non-nil *http.Response (a 502 is a completed round trip, never a
// reason to retry) and any request whose caller's ctx already ended, is
// returned as-is.
//
// Unlike a forward over a fixed local listener there is no reconnection to wait
// for: the retry simply dials again, re-resolving the address, so it can land
// on a different ready pod.
//
// "Retrying once" is a promise about this layer, not about deliveries:
// net/http's own Transport silently retries a request on a connection it reused
// from its idle pool if the server drops it before the response is readable.
// For a GET reused this way, that can mean the server sees the same request up
// to three times for one Do, once, once from Transport's own retry, and once
// from this one, which is safe only because every method this layer retries is
// also one Transport is willing to retry on its own.
//
// ctx is authoritative: Do runs the request under ctx, replacing whatever
// context req already carried, so a deadline or cancellation already on req is
// not consulted. context.Canceled and context.DeadlineExceeded are never
// swallowed: Do returns them so errors.Is sees them, and never retries a
// request that failed because ctx ended rather than because the dial or the
// stream did.
func (d *Dialer) Do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	return doRetry(ctx, client, req, "kubedial: do")
}

// doRetry runs req through client under ctx and retries once when the first
// attempt failed before any response arrived and the request is safe to send
// twice, reporting both attempts' errors tagged with prefix. It is the shared
// body of Dialer.Do and Listener.Do, which differ only in prefix, so the retry
// policy cannot drift between the two entry points.
func doRetry(ctx context.Context, client *http.Client, req *http.Request, prefix string) (*http.Response, error) {
	resp, err := client.Do(req.WithContext(ctx))
	if err == nil {
		return resp, nil
	}
	if resp != nil {
		// A completed round trip is never a reason to retry, even when it came
		// back with an error.
		return resp, fmt.Errorf("%s: %w", prefix, err)
	}
	firstErr := err

	// A cancelled or expired ctx surfaces from client.Do as a transport error
	// like any other, but it is the caller's own decision to stop, not evidence
	// the dial dropped, so it is never a reason to retry. Wrapping still lets
	// errors.Is reach it.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("%s: %w", prefix, err)
	}
	// A ctx that ended between the transport failing for an unrelated reason and
	// here also forbids the retry, and its own error stays reachable.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("%s: %w", prefix, ctxErr)
	}

	if !canRetry(req) {
		return nil, fmt.Errorf("%s: %w", prefix, err)
	}

	retry, rerr := cloneForRetry(ctx, req)
	if rerr != nil {
		return nil, rerr
	}

	// Exactly one retry: a second failure, successful or not, is final. Both
	// attempts' errors are reported, each wrapped, so a caller can tell a first
	// lookup or dial failure from a retry that never connected and errors.Is
	// reaches either one.
	resp, err = client.Do(retry)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("%s: %w", prefix, firstErr),
			fmt.Errorf("%s: retry: %w", prefix, err),
		)
	}
	return resp, nil
}

// canRetry reports whether req is safe to send a second time: an idempotent
// method, per retryableMethods, whose body (if any) can be rebuilt from
// scratch. http.NewRequest populates GetBody for a *bytes.Reader,
// *bytes.Buffer or *strings.Reader and leaves it nil for anything else, which
// is what makes this check real rather than a formality: an arbitrary io.Reader
// has already been drained by the failed first attempt. http.NoBody is a third
// case alongside a nil Body: http.NewRequest never sets GetBody for it, but it
// is a sentinel empty reader, safe to reuse as-is rather than something that
// needs replaying.
func canRetry(req *http.Request) bool {
	return retryableMethods[req.Method] &&
		(req.Body == nil || req.Body == http.NoBody || req.GetBody != nil)
}

// cloneForRetry builds the request the retry actually sends. req.Clone keeps
// the method, URL and headers; the body needs separate handling because
// req.Body was already read, and likely closed, by the failed first attempt, so
// the retry needs a fresh reader rather than the drained one. http.NoBody
// carries no state to drain, so Clone's copy of it is already fine to reuse.
func cloneForRetry(ctx context.Context, req *http.Request) (*http.Request, error) {
	clone := req.Clone(ctx)
	if req.Body == nil || req.Body == http.NoBody {
		return clone, nil
	}

	body, err := req.GetBody()
	if err != nil {
		return nil, fmt.Errorf("kubedial: rebuild request body for retry: %w", err)
	}
	clone.Body = body
	return clone, nil
}
