package kubedial

import "time"

// Option configures a Dialer.
type Option func(*config)

type config struct {
	clusterDomain string

	// namespace is "" by default, meaning bare in-cluster names resolve in
	// "default". WithNamespace sets it; a caller that wants the kubeconfig
	// context's namespace must pass that value itself, because a rest.Config
	// does not carry it.
	namespace string

	mode             Mode
	establishTimeout time.Duration
	positiveTTL      time.Duration
	negativeTTL      time.Duration
	podIdle          time.Duration

	// The fields below exist only so a test can replace the real clients, the
	// real transport and the real in-cluster probe. They have no exported
	// option on purpose: production callers must not be able to substitute a
	// partially working dialer.
	clients clients
	factory streamDialerFactory
	probe   func() bool
}

func defaultConfig() config {
	return config{
		clusterDomain:    "cluster.local",
		namespace:        "",
		mode:             ModeAuto,
		establishTimeout: 30 * time.Second,
		positiveTTL:      10 * time.Second,
		negativeTTL:      30 * time.Second,
		podIdle:          2 * time.Minute,
		probe:            inClusterProbe,
	}
}

// WithClusterDomain sets the DNS domain in-cluster names end with. The default
// is "cluster.local". An empty domain is not rejected here; New reports it.
func WithClusterDomain(domain string) Option {
	return func(c *config) { c.clusterDomain = domain }
}

// WithNamespace sets the namespace used for bare in-cluster names. Unset, bare
// names resolve in "default"; a rest.Config does not carry the kubeconfig
// context's namespace, so a caller that wants that namespace passes it here.
func WithNamespace(namespace string) Option {
	return func(c *config) { c.namespace = namespace }
}

// WithMode sets how the Dialer reaches addresses. The default is ModeAuto.
func WithMode(mode Mode) Option {
	return func(c *config) { c.mode = mode }
}

// WithEstablishTimeout sets how long a single port-forward may take to become
// ready. The default is 30s. A non-positive value is not rejected here; New
// reports it.
func WithEstablishTimeout(d time.Duration) Option {
	return func(c *config) { c.establishTimeout = d }
}

// WithPodIdle sets how long a pod's port-forward connection stays open with no
// streams on it before it is closed. The default is 2m. A longer value lets a
// caller with long gaps between dials reuse the connection rather than pay a
// fresh upgrade each time, at the cost of holding an idle connection open on
// the API server for that long. A non-positive value is not rejected here; New
// reports it.
func WithPodIdle(d time.Duration) Option {
	return func(c *config) { c.podIdle = d }
}

// withClients replaces the cluster clients a Dialer resolves through. Test-only.
func withClients(c clients) Option {
	return func(cfg *config) { cfg.clients = c }
}

// withStreamDialerFactory replaces the port-forward dialer a Dialer opens
// streams through. Test-only.
func withStreamDialerFactory(f streamDialerFactory) Option {
	return func(cfg *config) { cfg.factory = f }
}

// withInClusterProbe replaces the probe that decides an auto mode, so a test
// never reads the filesystem or the environment. Test-only.
func withInClusterProbe(probe func() bool) Option {
	return func(cfg *config) { cfg.probe = probe }
}
