package kubedial

import (
	"errors"
	"fmt"
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
