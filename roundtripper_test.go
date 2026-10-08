package kubedial

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	httpstream "k8s.io/streaming/pkg/httpstream"
)

// blockingRoundTripper blocks until its request context ends, modelling a round
// tripper whose only exit is its request's context ending.
type blockingRoundTripper struct{}

func (blockingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// TestEstablishDeadlineBoundsUpgrade checks that a next round tripper which
// blocks until its request context ends is bounded by the establish deadline.
// It models only that: a round tripper that consults its request's context while
// waiting. The real SpdyRoundTripper does not, since it reads the upgrade
// response straight off the connection, which is why the deadline it is given
// can bound connection establishment but not a server that goes silent after it.
func TestEstablishDeadlineBoundsUpgrade(t *testing.T) {
	const timeout = 50 * time.Millisecond
	rt := &establishDeadlineRoundTripper{next: blockingRoundTripper{}, timeout: timeout}
	client := &http.Client{Transport: rt}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.invalid/", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	if err == nil {
		//nolint:errcheck // the test fails next; the body is irrelevant.
		resp.Body.Close()
		t.Fatal("round trip succeeded, want a deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("round trip error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Errorf("round trip took %v, want it bounded near %v", elapsed, timeout)
	}
}

// errRoundTripper returns a fixed error, standing in for a dialer whose upgrade
// the server refused.
type errRoundTripper struct{ err error }

func (rt errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, rt.err
}

// TestUpgradeFailureTriggersFallbackNotTimeout pins the predicate pool.open
// installs for the websocket-to-SPDY fallback: an upgrade failure survives the
// round tripper's wrapping and still triggers the fallback, while an establish
// timeout does not, so a slow-but-working websocket path is not abandoned for
// SPDY on every slow API server.
func TestUpgradeFailureTriggersFallbackNotTimeout(t *testing.T) {
	fallback := func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	}
	newRequest := func(t *testing.T) *http.Request {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.invalid/", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		return req
	}

	upgradeErr := &httpstream.UpgradeFailureError{Cause: errors.New("bad status")}
	client := &http.Client{Transport: &establishDeadlineRoundTripper{next: errRoundTripper{err: upgradeErr}, timeout: time.Second}}
	resp, err := client.Do(newRequest(t))
	if resp != nil {
		//nolint:errcheck // the test fails next; the body is irrelevant.
		resp.Body.Close()
	}
	if !fallback(err) {
		t.Errorf("fallback(%v) = false, want true for a wrapped upgrade failure", err)
	}

	client = &http.Client{Transport: &establishDeadlineRoundTripper{next: blockingRoundTripper{}, timeout: 50 * time.Millisecond}}
	resp, err = client.Do(newRequest(t))
	if resp != nil {
		//nolint:errcheck // the test fails next; the body is irrelevant.
		resp.Body.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out round trip error = %v, want context.DeadlineExceeded", err)
	}
	if fallback(err) {
		t.Errorf("fallback(%v) = true, want false for an establish timeout", err)
	}
}
