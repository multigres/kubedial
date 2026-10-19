package kubedial

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// refusingAPIServer is an API server that serves one Service, its
// EndpointSlice and its pod, and refuses one kind of request with a fixed
// status. Everything above it is real client-go: the typed clients, the SPDY
// and websocket upgrade dialers and their fallback, so the error shapes under
// test are the ones client-go produces rather than ones a fake hand-builds.
type refusingAPIServer struct {
	// target names the request refused: "services", "endpointslices", "pods"
	// or "portforward".
	target string
	code   int
}

// refusalMessage is the Status message the server refuses with, so a test can
// prove the original error is still in the chain after classification.
const refusalMessage = "refused by the test API server"

func (s refusingAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var kind string
	switch {
	case strings.HasSuffix(r.URL.Path, "/portforward"):
		kind = "portforward"
	case strings.Contains(r.URL.Path, "/services/"):
		kind = "services"
	case strings.HasSuffix(r.URL.Path, "/endpointslices"):
		kind = "endpointslices"
	case strings.HasSuffix(r.URL.Path, "/pods"):
		kind = "pods"
	default:
		http.NotFound(w, r)
		return
	}

	if kind == s.target {
		reason := metav1.StatusReasonInternalError
		switch s.code {
		case http.StatusUnauthorized:
			reason = metav1.StatusReasonUnauthorized
		case http.StatusForbidden:
			reason = metav1.StatusReasonForbidden
		}
		writeJSON(w, s.code, &metav1.Status{
			TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status:   metav1.StatusFailure,
			Message:  refusalMessage,
			Reason:   reason,
			Code:     int32(s.code),
		})
		return
	}

	switch kind {
	case "services":
		svc := newService("ns", "web", "10.0.0.1")
		svc.TypeMeta = metav1.TypeMeta{Kind: "Service", APIVersion: "v1"}
		svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
		writeJSON(w, http.StatusOK, svc)
	case "endpointslices":
		slice := newSlice("ns", "web",
			[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
			endpoint("web-1", "10.1.0.1", nil),
		)
		writeJSON(w, http.StatusOK, &discoveryv1.EndpointSliceList{
			TypeMeta: metav1.TypeMeta{Kind: "EndpointSliceList", APIVersion: "discovery.k8s.io/v1"},
			Items:    []discoveryv1.EndpointSlice{*slice},
		})
	case "pods":
		writeJSON(w, http.StatusOK, &corev1.PodList{
			TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"},
			Items:    []corev1.Pod{*readyPod("ns", "web-1", "10.1.0.1")},
		})
	default:
		// No test reaches a successful upgrade: every portforward that is not
		// the refused target is never requested.
		http.Error(w, "unexpected portforward", http.StatusTeapot)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	//nolint:errcheck,errchkjson // a failed write surfaces as the client's decode error.
	json.NewEncoder(w).Encode(v)
}

// newRefusingDialer returns a forward-mode Dialer built by New from a real
// rest.Config pointing at a refusingAPIServer, and that config.
func newRefusingDialer(t *testing.T, target string, code int) (*Dialer, *rest.Config) {
	t.Helper()
	srv := httptest.NewServer(refusingAPIServer{target: target, code: code})
	t.Cleanup(srv.Close)

	cfg := &rest.Config{Host: srv.URL}
	d, err := New(cfg, WithMode(ModeForward), WithEstablishTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, cfg
}

// unauthorizedEntry is one public entry point a 401 can surface through. run
// returns the error that entry point reports for addr, or skips when the entry
// point never reaches the refused request for that target.
type unauthorizedEntry struct {
	name string
	run  func(t *testing.T, d *Dialer, cfg *rest.Config, target, addr string) error
}

var unauthorizedEntries = []unauthorizedEntry{
	{"Resolve", func(t *testing.T, d *Dialer, _ *rest.Config, target, addr string) error {
		if target == "portforward" {
			t.Skip("Resolve never upgrades")
		}
		_, _, err := d.Resolve(t.Context(), addr)
		return err
	}},
	{"DialContext", func(t *testing.T, d *Dialer, _ *rest.Config, _, addr string) error {
		return dialErr(d.DialContext(t.Context(), "tcp", addr))
	}},
	{"DialGRPC", func(t *testing.T, d *Dialer, _ *rest.Config, _, addr string) error {
		return dialErr(d.DialGRPC(t.Context(), addr))
	}},
	{"LookupHost", func(t *testing.T, d *Dialer, _ *rest.Config, target, addr string) error {
		// LookupHost reads the cluster only for a Service name: a pod IP is
		// returned unchanged, and it never upgrades.
		if target == "pods" || target == "portforward" {
			t.Skip("LookupHost makes no request for this target")
		}
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("SplitHostPort: %v", err)
		}
		_, err = d.LookupHost(t.Context(), host)
		return err
	}},
	{"HTTPClient", func(t *testing.T, d *Dialer, _ *rest.Config, _, addr string) error {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := d.HTTPClient(nil).Do(req)
		if err == nil {
			resp.Body.Close()
		}
		return err
	}},
	{"Do", func(t *testing.T, d *Dialer, _ *rest.Config, _, addr string) error {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := d.Do(t.Context(), d.HTTPClient(nil), req)
		if err == nil {
			resp.Body.Close()
		}
		return err
	}},
	{"Listen/ReadyCheck", func(t *testing.T, d *Dialer, _ *rest.Config, _, addr string) error {
		l, err := d.Listen(t.Context(), addr, WithReadyCheck(time.Second))
		if err == nil {
			l.Close()
		}
		return err
	}},
	{"Listen/OnDialError", func(t *testing.T, d *Dialer, _ *rest.Config, _, addr string) error {
		return listenerDialError(t, func(opts ...ListenOption) (*Listener, error) {
			return d.Listen(t.Context(), addr, opts...)
		})
	}},
	{"Listen/StopReason", func(t *testing.T, d *Dialer, _ *rest.Config, _, addr string) error {
		return listenerStopReason(t, func(opts ...ListenOption) (*Listener, error) {
			return d.Listen(t.Context(), addr, opts...)
		})
	}},
	{"ListenEndpoint/ReadyCheck", func(t *testing.T, d *Dialer, _ *rest.Config, target, _ string) error {
		if target != "portforward" {
			t.Skip("ListenEndpoint makes no lookup")
		}
		l, err := d.ListenEndpoint(t.Context(), refusedEndpoint, WithReadyCheck(time.Second))
		if err == nil {
			l.Close()
		}
		return err
	}},
	{"ListenEndpoint/OnDialError", func(t *testing.T, d *Dialer, _ *rest.Config, target, _ string) error {
		if target != "portforward" {
			t.Skip("ListenEndpoint makes no lookup")
		}
		return listenerDialError(t, func(opts ...ListenOption) (*Listener, error) {
			return d.ListenEndpoint(t.Context(), refusedEndpoint, opts...)
		})
	}},
	{"ListenEndpoint/StopReason", func(t *testing.T, d *Dialer, _ *rest.Config, target, _ string) error {
		if target != "portforward" {
			t.Skip("ListenEndpoint makes no lookup")
		}
		return listenerStopReason(t, func(opts ...ListenOption) (*Listener, error) {
			return d.ListenEndpoint(t.Context(), refusedEndpoint, opts...)
		})
	}},
	{"ResolveService", func(t *testing.T, _ *Dialer, cfg *rest.Config, target, _ string) error {
		if target != "services" && target != "endpointslices" {
			t.Skip("ResolveService makes no request for this target")
		}
		cs := mustClientset(t, cfg)
		_, err := ResolveService(t.Context(), cs.CoreV1().Services("ns"), cs.DiscoveryV1().EndpointSlices("ns"), "ns", "web", "http")
		return err
	}},
	{"ResolvePod", func(t *testing.T, _ *Dialer, cfg *rest.Config, target, _ string) error {
		if target != "pods" {
			t.Skip("ResolvePod makes no request for this target")
		}
		cs := mustClientset(t, cfg)
		_, err := ResolvePod(t.Context(), cs.CoreV1().Pods("ns"), "ns", "app=web", 8080)
		return err
	}},
}

// refusedEndpoint is the pod refusingAPIServer's EndpointSlice names.
var refusedEndpoint = Endpoint{Namespace: "ns", Pod: "web-1", PodIP: "10.1.0.1", Port: 8080}

func dialErr(conn net.Conn, err error) error {
	if err == nil {
		conn.Close()
	}
	return err
}

func mustClientset(t *testing.T, cfg *rest.Config) *kubernetes.Clientset {
	t.Helper()
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("clientset: %v", err)
	}
	return cs
}

// listenerDialError starts a listener that redials, connects once, and returns
// the error WithOnDialError received for that connection.
func listenerDialError(t *testing.T, listen func(...ListenOption) (*Listener, error)) error {
	t.Helper()
	dialErrs := make(chan error, 1)
	l, err := listen(WithOnDialError(func(e error) {
		select {
		case dialErrs <- e:
		default:
		}
	}))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()
	select {
	case err := <-dialErrs:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("WithOnDialError was not called")
		return nil
	}
}

// listenerStopReason starts a WithNoRedial listener, connects once, and
// returns the reason it stopped with.
func listenerStopReason(t *testing.T, listen func(...ListenOption) (*Listener, error)) error {
	t.Helper()
	l, err := listen(WithNoRedial())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	conn := listenTestDial(t, l.Addr())
	defer conn.Close()
	select {
	case <-l.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not stop")
	}
	return l.Err()
}

// TestErrUnauthorizedThroughEveryEntryPoint refuses each request kubedial
// makes, a lookup or the port-forward upgrade, with a 401, a 403 and a 500,
// and asserts that every public entry point reaching that request reports
// ErrUnauthorized for the 401 alone, with client-go's own error still in the
// chain.
func TestErrUnauthorizedThroughEveryEntryPoint(t *testing.T) {
	targets := []struct {
		target string
		addr   string
	}{
		// Qualified, so a 403 on the Service stays an error rather than
		// falling through to DNS the way it does for a bare name.
		{"services", "web.ns.svc:80"},
		{"endpointslices", "web.ns.svc:80"},
		{"pods", "10.1.0.1:8080"},
		{"portforward", "web.ns.svc:80"},
	}
	codes := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError}

	for _, tg := range targets {
		for _, code := range codes {
			for _, entry := range unauthorizedEntries {
				t.Run(tg.target+"/"+http.StatusText(code)+"/"+entry.name, func(t *testing.T) {
					d, cfg := newRefusingDialer(t, tg.target, code)
					err := entry.run(t, d, cfg, tg.target, tg.addr)
					if err == nil {
						t.Fatal("err = nil, want the refusal")
					}
					want := code == http.StatusUnauthorized
					if got := errors.Is(err, ErrUnauthorized); got != want {
						t.Fatalf("errors.Is(err, ErrUnauthorized) = %v, want %v; err = %v", got, want, err)
					}
					if !want {
						return
					}
					// Classification wraps, never replaces: a lookup keeps the
					// API error a caller may already match on, and the upgrade
					// keeps client-go's message.
					if tg.target != "portforward" && !apierrors.IsUnauthorized(err) {
						t.Errorf("apierrors.IsUnauthorized(err) = false, want the API error kept in the chain; err = %v", err)
					}
					if !strings.Contains(err.Error(), refusalMessage) {
						t.Errorf("err = %v, want it to carry the server's message %q", err, refusalMessage)
					}
				})
			}
		}
	}
}

// TestListenReadyCheckKeepsErrNotReady pins that a 401 on the ready check
// adds ErrUnauthorized without dropping the sentinel the check has always
// reported.
func TestListenReadyCheckKeepsErrNotReady(t *testing.T) {
	d, _ := newRefusingDialer(t, "portforward", http.StatusUnauthorized)
	_, err := d.ListenEndpoint(t.Context(), refusedEndpoint, WithReadyCheck(time.Second))
	if !errors.Is(err, ErrNotReady) || !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want both ErrNotReady and ErrUnauthorized", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want no context error chained", err)
	}
}
