# kubedial

kubedial is a Go library that reaches Kubernetes in-cluster addresses
(`svc.ns.svc:2379`, `pod.headless.ns.svc.cluster.local:5432`, a pod IP) from
outside the cluster, by resolving the address to a ready pod and opening a
Kubernetes port-forward stream to it. The same address that resolves inside a
cluster normally fails from a laptop; kubedial hands back the stream as a
`net.Conn` so the client never knows the difference. Its core is
`DialContext(ctx, network, addr) (net.Conn, error)`, so it drops into any client
that accepts a dial function (`net/http`, gRPC, pgx, etcd, go-redis) without
depending on any of them.

Prior art does not cover this: kubefwd rewrites `/etc/hosts` and needs root,
telepresence is a VPN, and client-go's `portforward` is a local-listener building
block rather than a dialer.

## Quick start

```go
d, err := kubedial.New(restCfg, kubedial.WithNamespace(ns))
if err != nil {
	return err
}
defer d.Close()
```

`restCfg` is a `*rest.Config` (for example from `clientcmd` or
`rest.InClusterConfig`). `WithNamespace` sets the namespace that bare names such
as `my-svc` resolve in. Without it, bare names resolve in `default`: a
`rest.Config` does not carry the kubeconfig context's namespace, so a caller
that wants that namespace must read it from its own kubeconfig loading (for
example `clientcmd.ClientConfig.Namespace`) and pass it to `WithNamespace`.

`New` builds the lookup clients and the port-forward transport once, from
`restCfg`, and never rebuilds them. With a static `BearerToken` in `restCfg` (an
EKS token lasts 15 minutes), forwards that are already open keep working after
the token expires, but every new pod connection and every lookup fails. A
`Dialer` that outlives its credential needs a `rest.Config` whose auth is applied
per request:

- an `ExecProvider`, which is what `clientcmd` builds for a kubeconfig `exec`
  user such as `aws eks get-token`. client-go ignores it when `BearerToken` is
  also set.
- a `BearerTokenFile`, which client-go re-reads every minute.
- a `WrapTransport` that sets the credential on each request itself.

A rejected credential (HTTP 401, on a lookup or on the port-forward upgrade)
matches `errors.Is(err, kubedial.ErrUnauthorized)` through every entry point; a
403 does not, because it is an RBAC refusal rather than an expired credential.
With a static credential, close the `Dialer` and build a new one from a fresh
`rest.Config`. With an `ExecProvider`, client-go refreshes the credential after
the 401 that revealed it, so the next call can succeed on the same `Dialer`.

A pod's port-forward connection closes after two minutes with no streams on it.
A caller with longer gaps between dials can raise that with `WithPodIdle`, at the
cost of an idle connection held open on the API server for that long.

net/http:

```go
client := d.HTTPClient(nil) // *http.Client with DialContext set to d.DialContext
resp, err := client.Get("http://my-svc.my-ns.svc:8080/healthz")
```

gRPC:

```go
conn, err := grpc.NewClient(
	"passthrough:///my-svc.my-ns.svc:2379",
	grpc.WithContextDialer(d.DialGRPC),
)
```

The `passthrough:///` prefix is not optional; see [Client traps](#client-traps).

pgx:

```go
cfg, err := pgx.ParseConfig(connStr)
if err != nil {
	return err
}
cfg.LookupFunc = d.LookupHost
cfg.DialFunc = d.DialContext
conn, err := pgx.ConnectConfig(ctx, cfg)
```

Again, both hooks are needed; see [Client traps](#client-traps).

## Modes

A `Dialer` has one mode, decided once at `New` and never changed.

- `ModeAuto` (the default): in-cluster (both a mounted service-account token and
  `KUBERNETES_SERVICE_HOST` are present) settles to `ModeDirect`; otherwise it
  settles to `ModeForward`. `Mode()` reports the resolved value and is never
  `ModeAuto`.
- `ModeDirect`: a plain `net.Dialer` for every address. This is also the right
  override on a laptop whose VPN routes pod IPs, via
  `WithMode(kubedial.ModeDirect)`.
- `ModeForward`: in-cluster addresses go through a port-forward stream; anything
  `Resolve` does not claim goes through a plain `net.Dialer`, so one dialer can
  sit under a transport that also talks to external hosts.

There is no try-direct-then-fall-back. Every miss costs a dial timeout, and a
silent switch from one path to the other turns a misconfiguration into "slow but
works". The mode is a decision, not a guess, and `Mode()` exists so a caller can
log it once.

## Addresses

The cluster DNS domain is configurable with `WithClusterDomain`; the default is
`cluster.local`.

| Address form | Resolution |
|---|---|
| `svc`, `svc.ns`, `svc.ns.svc`, `svc.ns.svc.<domain>` | Service port to a ready endpoint, via EndpointSlices |
| `pod.svc.ns.svc[.<domain>]` | Pod by hostname behind a headless Service (StatefulSet ordinals included) |
| A pod IP literal | Pod by field selector `status.podIP` |
| Anything else | Not in-cluster; dialled directly |

Port semantics differ by form:

- For a ClusterIP Service, the port in the address is a Service port. That
  Service port's name selects the endpoint port on the pod, so a Service that
  maps port 80 to target port 8080 is reached at the Service port, not the pod
  port.
- For a headless Service and for the headless-pod and pod-IP forms, the port in
  the address is the pod port, used as is.

A one- or two-label name (`svc`, `svc.ns`) counts as in-cluster only if the
Service actually exists; otherwise it is an ordinary host name and is dialled
directly. A `Forbidden` from the `get services` that decides this is treated the
same way, as "not known to exist", so a client without cluster-wide
`get services` sends an unqualified name to real DNS and dials it directly; that
is how an external name such as `example.com` still works. A fully qualified
name (`svc.ns.svc`, `svc.ns.svc.<domain>`) is a cluster claim: if the Service
does not exist, resolution reports `ErrNotFound` rather than falling through to
DNS, and a `Forbidden` stays an error rather than becoming a DNS failure.

Loopback IP literals are never forwarded and always dial directly, before any
cluster call.

## Client traps

Some clients resolve a name before they call a custom dialer. Each of these has
a shape that looks right and fails.

### gRPC

`grpc.NewClient` resolves its target through the default `dns` resolver before
it calls a `WithContextDialer` hook, so `<pod>.<headless>.<ns>.svc.cluster.local`
fails on a laptop and the dialer is never reached. Use a `passthrough:///`
target so no resolution happens and the hostname is handed to the dialer and kept
as the `:authority`:

```go
conn, err := grpc.NewClient(
	"passthrough:///my-svc.my-ns.svc:2379",
	grpc.WithContextDialer(d.DialGRPC),
)
```

An IP-literal target is the exception: `dns` passes it through without a lookup,
so no prefix is needed:

```go
conn, err := grpc.NewClient("10.0.0.7:1234", grpc.WithContextDialer(d.DialGRPC))
```

`DialGRPC` is a method, not a gRPC-typed helper, so the core carries no gRPC
dependency.

### pgx

pgx has two traps, both in `pgconn`.

It resolves before it dials: `pgconn` calls the configured `LookupFunc` and
passes the result to `DialFunc`, so the default lookup of
`<svc>.<ns>.svc` fails on a laptop before the dialer runs. Set both hooks:

```go
cfg, err := pgx.ParseConfig(connStr)
if err != nil {
	return err
}
cfg.LookupFunc = d.LookupHost
cfg.DialFunc = d.DialContext
conn, err := pgx.ConnectConfig(ctx, cfg)
```

kubedial's `LookupHost` returns an in-cluster name unchanged (so the client hands
it back to `DialContext`) and does real DNS for everything else.

Cancel requests redial `RemoteAddr()`: `PgConn.CancelRequest` dials
`conn.RemoteAddr().String()` through `DialFunc`. A cancel must reach the same
backend, and re-resolving a Service name could pick another pod and cancel
nothing. kubedial's `RemoteAddr` therefore returns `tcp` and `<podIP>:<port>` of
the pod the stream actually reached, and `DialContext` on that string reaches
that same pod. `RemoteAddr` is a contract here, not a logging nicety.

That contract is exercised only if pgx actually sends a cancel request. In pgx
v5.10.0 the default context watcher is `pgconn.DeadlineContextWatcherHandler`,
which only sets a local deadline on the connection and never sends a cancel
request; `CancelRequest`, the path that dials `RemoteAddr()` through `DialFunc`,
runs only when the caller installs `pgconn.CancelRequestContextWatcherHandler`,
or when a connection is closed after a fatal error. A caller who relies on a
cancel reaching the same backend must install it:

```go
cfg.BuildContextWatcherHandler = func(c *pgconn.PgConn) ctxwatch.Handler {
	return &pgconn.CancelRequestContextWatcherHandler{Conn: c, DeadlineDelay: 5 * time.Second}
}
```

(`pgconn` is `github.com/jackc/pgx/v5/pgconn` and `ctxwatch` is
`github.com/jackc/pgx/v5/pgconn/ctxwatch`.) Without the handler, kubedial's
`RemoteAddr` guarantee is never exercised: pgx applies a local deadline instead
of cancelling the query on the server.

### net/http

```go
client := d.HTTPClient(tlsCfg) // nil tlsCfg means platform roots
```

`HTTPClient` returns an `*http.Client` whose transport is `d.Transport(tlsCfg)`;
that transport routes per host: requests to in-cluster names skip `HTTPS_PROXY`,
and requests to external hosts honour it. kubedial never sets
`InsecureSkipVerify`; pass a `*tls.Config` if you need custom verification, and
it is cloned. `d.Do(ctx, client, req)` retries once, only for idempotent methods
with a replayable body, never after a response has arrived and never when the
caller's `ctx` ended.

## Listen

Some tools can only be given an address: `psql`, a browser, a subprocess. For
those, `Listen` binds a stable local address and dials the target fresh for every
accepted connection:

```go
l, err := d.Listen(ctx, "my-svc.my-ns.svc:5432")
if err != nil {
	return err
}
defer l.Close()
fmt.Println(l.Addr()) // 127.0.0.1:<port>, stable for the listener's life
```

Each accepted connection dials its upstream on that connection's own goroutine,
so a connection whose dial is stuck does not delay the next accepted connection
or any other; a warm connection costs one stream open instead.

To pin a specific target instead of resolving the address each time, use
`ListenEndpoint` with an `Endpoint`. `Endpoint` values come from the
`Dialer.Resolve` method, or from the package-level functions
`ResolveService(ctx, svcs, slices, namespace, name, port)` and
`ResolvePod(ctx, pods, namespace, selector, port)`, which take caller-built typed
clients (`ServiceGetter`/`EndpointSliceLister` and `PodLister`).
`ListenEndpoint` errors in direct mode, where there is no forward pool to open.
`WithLocalPort` chooses the local port (the default is kernel-chosen).
`WithNoRedial` makes the listener stop after the first forward failure rather
than dial again, which is what a pinned target such as a database primary needs:
silently redialling after a failover would reach a standby. The pinned pod's
forward ending is told from one stream ending on a live pod (a Postgres cancel
request, a server-side terminate) by whether the port-forward connection still
carries another stream: only the whole forward ending leaves none, so a cancel
request does not stop a listener that still has a session on it. `WithReadyCheck`
probes the target once before `Listen` returns and reports `ErrNotReady` if the
forward cannot be opened or the kubelet refuses it. It treats silence as ready,
and a healthy forward says nothing until the client speaks, so it waits out the
full timeout on every healthy target and returns early only when the kubelet
writes a forward error or the connection is lost. The timeout is therefore a
per-command latency budget, not a backstop: a large value buys a clear refusal
error at the cost of delaying every `Listen` by up to that value.
`WithOnDialError` observes per-dial errors. The listener's lifetime is `ctx`;
when `ctx` ends it stops and
`Err()` reports `ErrStopped`. `Done`/`Err` report when and why it stopped, and
`HTTPClient`/`Do` are available on the listener too.

A cancel request through a `Listener` is a new loopback connection, so it opens a
new upstream connection and re-resolves the address, and can reach a different
pod than the one being cancelled; use `ListenEndpoint` when a cancel must reach
the same backend. That is the shape a cancellable database connection wants:
pin it with `ListenEndpoint` and `WithNoRedial` so its cancel request reaches the
same pod and a failover stops the listener rather than redialling a standby.

## RBAC

Forward mode needs:

- `create` on `pods/portforward`
- `get` on `services`
- `list` on `endpointslices.discovery.k8s.io`
- `list` on `pods`, cluster-wide for pod-IP addresses. Without the cluster-wide
  list, pod-IP resolution falls back to the Dialer's namespace (`WithNamespace`,
  else `default`), so a pod IP in another namespace will not resolve.

Direct mode needs none of this. In-cluster addresses never leave the cluster in
that mode.

## Optional linking: a documented build-tag shim

A consumer that wants kubedial for development but not in production binaries
copies a small shim into its own tree. kubedial does not ship it. The point is
that the consumer's code sees only `dial.Dialer`, so no kubedial type escapes the
shim, while `*kubedial.Dialer` satisfies that interface directly.

```go
// internal/dial/dial.go
type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	LookupHost(ctx context.Context, host string) ([]string, error)
	Transport(tlsConfig *tls.Config) *http.Transport
}

// internal/dial/default.go
//go:build !kubedial
type plain struct{ net.Dialer }

func (p plain) LookupHost(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}
func (plain) Transport(tlsConfig *tls.Config) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = tlsConfig.Clone()
	return t
}
// Only *plain satisfies Dialer: the embedded net.Dialer's DialContext has a pointer receiver.
func New(*rest.Config) (Dialer, error) { return &plain{}, nil }

// internal/dial/kubedial.go
//go:build kubedial
func New(cfg *rest.Config) (Dialer, error) {
	d, err := kubedial.New(cfg)
	if err != nil {
		return nil, err
	}
	return d, nil
}
```

Development builds pass `-tags kubedial`; release builds do not, and link a plain
`net.Dialer`.

The interface has three methods rather than a bare dial function for two
reasons. `Transport` exists because building one in consumer code from a dial
function loses kubedial's per-host proxy rule (in-cluster names skip the proxy,
external hosts honour `HTTPS_PROXY`). `LookupHost` exists because some clients
resolve names before calling their dial hook (pgx), and a laptop cannot resolve
in-cluster names. Every client-specific adapter is then a one-liner over these.

The shim lives in the consumer, not in a `kubedial/optional` package, because
such a package would still link a package from the kubedial module into the
untagged build, so `go version -m` and SBOMs generated from the binary would list
kubedial anyway. That defeats the point.

**What the shim does not hide.** `go mod tidy` evaluates all build tags, so the
consumer's `go.mod` still requires kubedial even though the release binary does
not contain it. The binary is clean; the manifest is not. Avoiding that needs a
separate module, which is not worth it.

**Bitrot.** Plain `go vet`, golangci-lint, gopls and `go test` all skip the
tagged file, so the kubedial build of the shim can drift out of compilation. Add
a CI job that vets and lints with the tag:

```sh
go vet -tags kubedial ./...
golangci-lint run --build-tags kubedial ./...
```

The alternative is a separate `cmd/<name>-dev` main that imports kubedial with no
tags. It avoids the bitrot but requires the consumer's `main()` to be callable as
a package first. The build tag is the recommended shape.

## Development

```sh
make check
make kind-up && make e2e && make kind-down
```

`make check` runs `go vet`, golangci-lint and the race-enabled test suite.
`make kind-up` creates a Kind cluster, `make e2e` runs the cluster-backed tests
under the `e2e` build tag, and `make kind-down` removes the cluster.
