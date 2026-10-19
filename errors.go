package kubedial

import (
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

var (
	// ErrNoReadyEndpoint reports that a Service resolved to no ready endpoint.
	ErrNoReadyEndpoint = errors.New("kubedial: no ready endpoint")

	// ErrPortNotFound reports that a Service port name has no matching endpoint
	// port.
	ErrPortNotFound = errors.New("kubedial: port not found")

	// ErrNotFound reports that an in-cluster name, such as a Service, does not
	// exist.
	ErrNotFound = errors.New("kubedial: in-cluster name not found")

	// ErrAmbiguousPodIP reports that a pod IP matched more than one pod.
	ErrAmbiguousPodIP = errors.New("kubedial: pod IP matches more than one pod")

	// ErrUnsupportedService reports that a Service type, such as ExternalName,
	// cannot be reached with a port-forward.
	ErrUnsupportedService = errors.New("kubedial: service type cannot be forwarded")

	// ErrUnsupportedNetwork reports that a dial network other than "tcp" was
	// requested.
	ErrUnsupportedNetwork = errors.New("kubedial: network cannot be forwarded")

	// ErrNotReady reports that a forward did not become ready in time.
	ErrNotReady = errors.New("kubedial: forward did not become ready")

	// ErrStopped reports that a listener stopped. A Dialer, or its pool, being
	// closed reports it too, so a dial through a closed Dialer fails with
	// ErrStopped even though no listener is involved.
	ErrStopped = errors.New("kubedial: listener stopped")

	// ErrUnauthorized reports that the API server rejected the Dialer's
	// credential with a 401, on a lookup or on a port-forward upgrade. It is
	// how a caller holding a long-lived Dialer tells an expired credential from
	// any other failure; the package documentation's credential lifetime
	// section says what to do about it. A 403 is an RBAC refusal, not an
	// expired credential, and does not match.
	ErrUnauthorized = errors.New("kubedial: unauthorized")
)

// ForwardError carries the message the kubelet wrote on a port-forward's error
// stream, e.g. a refused connection inside the pod.
type ForwardError struct {
	Namespace string
	Pod       string
	Port      int32
	Msg       string
}

func (e *ForwardError) Error() string {
	return fmt.Sprintf("kubedial: forward to %s/%s:%d: %s", e.Namespace, e.Pod, e.Port, e.Msg)
}

// unauthorizedError marks err as a 401 for errors.Is(err, ErrUnauthorized)
// while keeping err's text and chain intact, so a caller that already matches
// on the API error (apierrors.IsUnauthorized) or logs the message sees no
// change.
type unauthorizedError struct{ err error }

func (e *unauthorizedError) Error() string { return e.err.Error() }

func (e *unauthorizedError) Unwrap() error { return e.err }

func (e *unauthorizedError) Is(target error) bool { return target == ErrUnauthorized }

// classifyAPIError marks an API error that is a 401 as ErrUnauthorized and
// returns any other error unchanged.
func classifyAPIError(err error) error {
	if err != nil && apierrors.IsUnauthorized(err) {
		return &unauthorizedError{err: err}
	}
	return err
}
