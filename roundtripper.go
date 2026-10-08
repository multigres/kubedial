package kubedial

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// establishDeadlineRoundTripper puts a deadline on the request context of a
// streaming upgrade request, on the websocket path as well as the SPDY one.
//
// It is the only place the deadline can go. Neither dialer carries a caller
// context into the request: spdy's builds it with http.NewRequest rather than
// http.NewRequestWithContext, so it arrives carrying context.Background(), and
// the websocket dialer builds a gorilla dialer that reaches the network through
// the request context, which the tunneling dialer likewise sets to
// context.Background(). Both reach the network only through req.Context() --
// net.Dialer.DialContext and tls.Dialer.DialContext when there is no proxy, the
// CONNECT and SOCKS5 paths when there is. Without this, an API server that
// never completes the handshake leaves the forward blocked forever, and closing
// it, which joins that goroutine, blocks with it.
//
// http.Client.Timeout is not used for the same job: its timer spans the whole
// round trip, which for an upgrade includes the hijacked connection the forward
// then lives on.
//
// The bound covers the upgrade handshake only. Once the connection is up, no
// downstream code consults this context again: on the SPDY path SpdyRoundTripper
// reads the upgrade response straight off the raw net.Conn and consults no
// context while doing so, so a server that finishes the TCP and TLS handshakes
// and then goes silent is bounded by TCP keep-alive rather than by this
// deadline.
//
// Ported from supa-client's externalconnect, where ForwardPorts and
// Forwarder.Close are that package's names for what kubedial calls pool.open and
// pool.Close; there it wrapped the SPDY transport alone, here it wraps the
// websocket transport too.
type establishDeadlineRoundTripper struct {
	next    http.RoundTripper
	timeout time.Duration
}

func (rt *establishDeadlineRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(req.Context(), rt.timeout)
	// Cancelling here, before the caller has read the response, is safe
	// precisely because of the limitation above: once the connection is up,
	// nothing downstream looks at this context again.
	defer cancel()

	resp, err := rt.next.RoundTrip(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("streaming upgrade request: %w", err)
	}
	return resp, nil
}

// WrappedRoundTripper keeps client-go's transport-unwrapping helpers able to
// see through to the real round tripper.
func (rt *establishDeadlineRoundTripper) WrappedRoundTripper() http.RoundTripper {
	return rt.next
}
