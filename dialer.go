package kubedial

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	discoveryv1client "k8s.io/client-go/kubernetes/typed/discovery/v1"
	"k8s.io/client-go/rest"
)

// serviceAccountTokenPath is where a pod's service account token is mounted.
const serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// defaultNamespace is used for bare in-cluster names when no namespace is set.
const defaultNamespace = "default"

// inClusterProbe reports whether the process runs inside a Kubernetes pod. Both
// the mounted token and the API host are checked: a laptop with a stale token
// file, or a host process with the variable exported, must not be mistaken for
// a pod.
func inClusterProbe() bool {
	if _, err := os.Stat(serviceAccountTokenPath); err != nil {
		return false
	}
	return os.Getenv("KUBERNETES_SERVICE_HOST") != ""
}

// hostResolver is the resolver LookupHost falls back to when an address is not
// an in-cluster name. It is an interface, held in an unexported field, so a
// test can supply one and no test performs a real lookup.
type hostResolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// Dialer resolves Kubernetes in-cluster addresses and dials the pod they name.
// The mode is decided exactly once, in New, and never changes: there is no
// try-direct-then-fall-back, because a dial that silently switches paths hides
// which path is actually being exercised.
type Dialer struct {
	mode Mode
	conf config

	// direct is the plain dialer both modes use for addresses that resolve to
	// ok=false (and everything, in ModeDirect).
	direct net.Dialer

	resolver hostResolver

	pool  *pool         // nil in ModeDirect
	cache *resolveCache // nil in ModeDirect

	// pick chooses among the ready candidates of an address, uniformly at random
	// by default, so new dials spread across replicas. It is per-Dialer, never a
	// package-level source, and a test replaces it to make the pick deterministic.
	pick func(n int) int

	stopped atomic.Bool
}

// New returns a Dialer. cfg may be nil in direct mode; in forward mode a nil
// cfg is an error, because both the typed clients and the port-forward
// transport are built from it.
//
// A rest.Config does not carry the kubeconfig context's namespace. Callers that
// want bare names to default to it must pass WithNamespace from their own
// kubeconfig loading; otherwise bare names resolve in "default".
func New(cfg *rest.Config, opts ...Option) (*Dialer, error) {
	conf := defaultConfig()
	for _, opt := range opts {
		opt(&conf)
	}
	if err := conf.validate(); err != nil {
		return nil, err
	}

	mode := conf.mode
	if mode == ModeAuto {
		if conf.probe() {
			mode = ModeDirect
		} else {
			mode = ModeForward
		}
	}

	var (
		p     *pool
		cache *resolveCache
	)
	if mode == ModeForward {
		if cfg == nil {
			return nil, errors.New("kubedial: forward mode requires a rest config")
		}

		// The typed clients are only built when a test has not supplied fakes;
		// the core client is also what the port-forward factory dials through,
		// so it is built whenever either is missing.
		var core corev1client.CoreV1Interface
		if conf.clients == nil || conf.factory == nil {
			var err error
			core, err = corev1client.NewForConfig(cfg)
			if err != nil {
				return nil, fmt.Errorf("kubedial: build core client: %w", err)
			}
		}
		if conf.clients == nil {
			disc, err := discoveryv1client.NewForConfig(cfg)
			if err != nil {
				return nil, fmt.Errorf("kubedial: build discovery client: %w", err)
			}
			conf.clients = restClients{core: core, disc: disc}
		}
		factory := conf.factory
		if factory == nil {
			var err error
			factory, err = newRESTStreamDialerFactory(cfg, core, conf.establishTimeout)
			if err != nil {
				return nil, err
			}
		}
		p = newPool(factory, conf.podIdle)
		cache = newResolveCache(time.Now, conf.positiveTTL, conf.negativeTTL)
	}

	return &Dialer{
		mode:     mode,
		conf:     conf,
		resolver: net.DefaultResolver,
		pool:     p,
		cache:    cache,
		pick:     newPicker(),
	}, nil
}

// Mode reports the resolved mode. It is never ModeAuto: New settles an auto
// choice to ModeDirect or ModeForward.
func (d *Dialer) Mode() Mode { return d.mode }

// Resolve reports the pod and port an address names, with ok=false for an
// address that must be dialled directly.
//
// In direct mode nothing is forwarded, so ok is always false and no lookup is
// made; the address is still parsed, so a malformed one still errors. The dial
// path never calls Resolve in direct mode.
func (d *Dialer) Resolve(ctx context.Context, addr string) (Endpoint, bool, error) {
	if d.mode == ModeDirect {
		if _, err := parseAddr(addr, d.conf.clusterDomain); err != nil {
			return Endpoint{}, false, err
		}
		return Endpoint{}, false, nil
	}
	return d.resolve(ctx, addr)
}

// resolve is the forward-mode path: cache, then parse, then the cluster. Errors
// are returned unwrapped to the cache, which never stores them; a caller sees
// the same error on every attempt until the cluster changes.
func (d *Dialer) resolve(ctx context.Context, addr string) (Endpoint, bool, error) {
	if ep, ok, found := d.cache.get(addr); found {
		return ep, ok, nil
	}

	// Capture the generation before the lookup so a write that lands after an
	// invalidate raced the lookup is dropped by put rather than re-caching a
	// dead endpoint.
	gen := d.cache.generation(addr)
	p, err := parseAddr(addr, d.conf.clusterDomain)
	if err != nil {
		return Endpoint{}, false, err
	}
	if isLoopbackAddr(p) {
		// A loopback address is never forwarded. Recording the negative result
		// keeps the loopback check off the hot path on repeat dials.
		d.cache.put(addr, Endpoint{}, false, gen)
		return Endpoint{}, false, nil
	}

	eps, ok, err := lookupAll(ctx, d.conf.clients, p, d.namespace())
	if err != nil {
		return Endpoint{}, false, err
	}
	if !ok || len(eps) == 0 {
		d.cache.put(addr, Endpoint{}, false, gen)
		return Endpoint{}, false, nil
	}

	// Spread new dials across the ready candidates: the pick is cached for the
	// positive TTL, so a long-lived connection stays pinned while dials in later
	// TTL windows land on other replicas. A single candidate is taken as is, so
	// the picker is not consulted for the pod-IP and headless-pod kinds.
	ep := eps[0]
	if len(eps) > 1 {
		ep = eps[d.pick(len(eps))]
	}
	// Also key the endpoint by the address RemoteAddr reports, so a redial to
	// that address (pgx sends its cancel there) is a cache hit and needs no
	// cluster call. On dual-stack the endpoint's family may not be the one
	// status.podIP names, so a fresh lookup by pod IP would find nothing.
	//
	// The RemoteAddr key has its own generation, and it is only known after the
	// pick, so its generation cannot be captured before the lookup the way the
	// address's is. Gate the second write on the first having landed instead: a
	// lookup invalidated mid-flight drops the address write, and the derived
	// write must drop with it rather than re-cache the same dead endpoint under
	// podIP:port.
	if d.cache.put(addr, ep, true, gen) {
		if ra := remoteAddrString(ep); ra != "" {
			d.cache.put(ra, ep, true, d.cache.generation(ra))
		}
	}
	return ep, true, nil
}

// DialContext dials addr. In direct mode it is a plain net.Dialer. In forward
// mode an address that resolves in-cluster is reached over a port-forward
// stream and everything else is dialled directly.
func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.stopped.Load() {
		return nil, ErrStopped
	}
	if d.mode == ModeDirect {
		conn, err := d.direct.DialContext(ctx, network, addr)
		if err != nil {
			return nil, fmt.Errorf("kubedial: dial %s: %w", addr, err)
		}
		return conn, nil
	}
	return d.dialForward(ctx, network, addr)
}

// DialGRPC is DialContext for gRPC's dial hook, which wants only an address.
func (d *Dialer) DialGRPC(ctx context.Context, addr string) (net.Conn, error) {
	return d.DialContext(ctx, "tcp", addr)
}

func (d *Dialer) dialForward(ctx context.Context, network, addr string) (net.Conn, error) {
	ep, ok, err := d.Resolve(ctx, addr)
	if err != nil {
		return nil, err
	}
	if !ok {
		conn, err := d.direct.DialContext(ctx, network, addr)
		if err != nil {
			return nil, fmt.Errorf("kubedial: dial %s: %w", addr, err)
		}
		return conn, nil
	}

	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return nil, fmt.Errorf("kubedial: dial %s: network %q: %w", addr, network, ErrUnsupportedNetwork)
	}

	data, errStream, pc, release, err := d.pool.openConn(ctx, ep)
	if err != nil {
		// The caller's own context ending is not evidence against the address,
		// so keep the entry and save the retry a re-resolve. Any other failure
		// may be a stale pod, so drop the entry and resolve again next time.
		if ctx.Err() == nil {
			d.cache.invalidate(addr)
		}
		return nil, fmt.Errorf("kubedial: dial %s: %w", addr, err)
	}
	// The sibling callback must use the same post-release semantics as the one
	// ListenEndpoint builds (pool.streamsOpenAfterRelease): pipe asks it only
	// after releasing this stream's own reference.
	conn := newStreamConn(ep, data, errStream, release, func() bool { return d.pool.streamsOpenAfterRelease(pc) })
	// A kubelet ForwardError on an established stream means the pod is no longer
	// a good answer, exactly as an open failure does, so the next dial resolves
	// again. newStreamConn only ever returns a *streamConn, but it is typed as
	// net.Conn so the assertion is needed to reach the hook.
	if sc, isStream := conn.(*streamConn); isStream {
		sc.invalidateCacheOnForwardError(func() { d.cache.invalidate(addr) })
	}
	return conn, nil
}

// LookupHost resolves host for clients that resolve a name before calling their
// dial hook, pgx among them. An in-cluster name is returned unchanged so the
// client hands it back to DialContext; returning its pod IP would make the
// client dial that IP directly and bypass the forward.
func (d *Dialer) LookupHost(ctx context.Context, host string) ([]string, error) {
	if d.mode == ModeForward {
		p := parseHost(host, d.conf.clusterDomain)
		switch p.Kind {
		case kindPodIP, kindHeadlessPod:
			return []string{host}, nil
		case kindService:
			// A name is in-cluster only if it names an existing Service. lookup
			// returns ok=false for an unqualified name that does not, which has
			// to go to real DNS. A host has no port, so lookup's port checks
			// cannot succeed; ErrPortNotFound, ErrNoReadyEndpoint and
			// ErrUnsupportedService all mean the Service was read and therefore
			// exists, which is all LookupHost needs to keep the name intact.
			// Every other error, ErrNotFound among them for a qualified name
			// that does not exist, propagates: a qualified name asserts
			// in-cluster intent, so sending it to DNS would hide a missing
			// Service behind a DNS failure.
			//
			// The verdict is cached under the bare host, which carries no port
			// and so cannot collide with an address key. Both halves are cached:
			// the "is a Service" verdict under ok=true, and the "is not a
			// Service" verdict under ok=false for the negative TTL. pgx resolves
			// on every connect, so without the positive half it would pay a
			// Service Get and an EndpointSlice List each time, and without the
			// negative half an unqualified two-label external name (example.com
			// parses as Service example in namespace com) would pay a Service
			// Get on every connect.
			_, exists, found := d.cache.get(host)
			if found && exists {
				return []string{host}, nil
			}
			if !found {
				gen := d.cache.generation(host)
				_, ok, err := lookup(ctx, d.conf.clients, p, d.namespace())
				if err != nil {
					if errors.Is(err, ErrPortNotFound) || errors.Is(err, ErrNoReadyEndpoint) || errors.Is(err, ErrUnsupportedService) {
						d.cache.put(host, Endpoint{}, true, gen)
						return []string{host}, nil
					}
					return nil, err
				}
				if ok {
					d.cache.put(host, Endpoint{}, true, gen)
					return []string{host}, nil
				}
				// Not an in-cluster Service: cache the negative verdict so the
				// next connect goes straight to real DNS.
				d.cache.put(host, Endpoint{}, false, gen)
			}
		}
	}
	addrs, err := d.resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("kubedial: lookup host %s: %w", host, err)
	}
	return addrs, nil
}

// Close closes the port-forward pool. Dials after Close return ErrStopped.
// Close is idempotent.
func (d *Dialer) Close() error {
	d.stopped.Store(true)
	if d.pool == nil {
		return nil
	}
	return d.pool.Close()
}

// namespace is the namespace bare in-cluster names resolve in.
func (d *Dialer) namespace() string {
	if d.conf.namespace != "" {
		return d.conf.namespace
	}
	return defaultNamespace
}

// newPicker returns the default spread picker: a uniform choice over n from a
// pseudo-random source private to one Dialer. math/rand/v2's global source
// cannot be seeded by a test, and the per-Dialer source means a test can never
// steer the default at all; the mutex guards it because resolves run
// concurrently.
func newPicker() func(n int) int {
	var mu sync.Mutex
	r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	return func(n int) int {
		mu.Lock()
		defer mu.Unlock()
		return r.IntN(n)
	}
}

// remoteAddrString renders an endpoint the way streamConn.RemoteAddr does, so
// the dialer can cache the endpoint under the address a caller would redial.
func remoteAddrString(ep Endpoint) string {
	ip := net.ParseIP(ep.PodIP)
	if ip == nil {
		return ""
	}
	return (&net.TCPAddr{IP: ip, Port: int(ep.Port)}).String()
}

// isLoopbackAddr reports whether a parsed address names a loopback IP. Only an
// IP literal can: a loopback must never be reached through a pod's port-forward,
// and the check runs before any lookup, so a loopback never costs a client call.
func isLoopbackAddr(p parsedAddr) bool {
	if p.Kind != kindPodIP {
		return false
	}
	ip := net.ParseIP(p.Host)
	return ip != nil && ip.IsLoopback()
}

// restClients adapts the two narrow typed clients to the namespace-scoped
// surface lookup needs. Building it from the narrow packages rather than
// kubernetes.NewForConfig keeps the dependency on discovery off the core client.
type restClients struct {
	core corev1client.CoreV1Interface
	disc discoveryv1client.DiscoveryV1Interface
}

func (r restClients) Services(ns string) ServiceGetter { return r.core.Services(ns) }

func (r restClients) Pods(ns string) PodLister { return r.core.Pods(ns) }

func (r restClients) EndpointSlices(ns string) EndpointSliceLister {
	return r.disc.EndpointSlices(ns)
}

// validate rejects a config New cannot use. The exported options deliberately
// do not reject these themselves so the error names New as its origin and every
// option is reported from one place.
func (c config) validate() error {
	if c.clusterDomain == "" {
		return errors.New("kubedial: cluster domain must not be empty")
	}
	if c.establishTimeout <= 0 {
		return fmt.Errorf("kubedial: establish timeout must be positive, got %s", c.establishTimeout)
	}
	switch c.mode {
	case ModeAuto, ModeDirect, ModeForward:
	default:
		return fmt.Errorf("kubedial: unknown mode %d", int(c.mode))
	}
	return nil
}
