//go:build e2e

// Package e2e exercises kubedial against a real Kind cluster. It is the only
// place in this module a cluster is touched, and the build tag keeps it out of
// the ordinary suite.
package e2e

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/multigres/kubedial"
)

const (
	namespace     = "kubedial-e2e"
	kubeContext   = "kind-e2e-kubedial"
	clusterDomain = "cluster.local"
)

// TestMain applies the fixtures before any test runs. The cluster's own
// lifecycle belongs to `make kind-up` / `make kind-down`; this only fills it.
func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := applyManifests(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: "+err.Error())
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func applyManifests(ctx context.Context) error {
	if _, err := kubectl(ctx, "apply", "-f", "manifests.yaml"); err != nil {
		return err
	}
	return waitReady(ctx)
}

// waitReady retries the wait because `kubectl apply` returning does not mean the
// controllers have created the pods yet, and a wait against an empty list is an
// error rather than a wait.
func waitReady(ctx context.Context) error {
	deadline := time.Now().Add(3 * time.Minute)
	for {
		out, err := kubectl(ctx, "wait", "--for=condition=Ready", "pod", "--all", "-n", namespace, "--timeout=180s")
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || !strings.Contains(string(out), "no matching resources found") {
			return err
		}
		time.Sleep(2 * time.Second)
	}
}

func kubectl(ctx context.Context, args ...string) ([]byte, error) {
	full := append([]string{"--context", kubeContext}, args...)
	out, err := exec.CommandContext(ctx, "kubectl", full...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return out, nil
}

// TestE2EHTTPThroughService reaches a pod through a ClusterIP Service's port,
// which is the mapping the address table promises: the Service port 80 lands on
// the pod port 8080.
func TestE2EHTTPThroughService(t *testing.T) {
	d := newDialer(t)
	names := webPodNames(t)
	body := getBody(t, d.HTTPClient(nil), "http://web."+namespace+".svc:80/hostname")
	if !names[body] {
		t.Fatalf("response %q is not one of the web pods %v", body, names)
	}
}

// TestE2EHTTPShortName is the bare-name form, resolved in the namespace the
// dialer was configured with rather than in "default".
func TestE2EHTTPShortName(t *testing.T) {
	d := newDialer(t)
	names := webPodNames(t)
	body := getBody(t, d.HTTPClient(nil), "http://web:80/hostname")
	if !names[body] {
		t.Fatalf("response %q is not one of the web pods %v", body, names)
	}
}

// TestE2EHeadlessPodName dials one StatefulSet ordinal through its headless
// Service name, the address form cluster DNS serves for a single pod.
func TestE2EHeadlessPodName(t *testing.T) {
	d := newDialer(t)
	ctx := t.Context()

	cfg := pgxConfig(t, "pg-0.pg."+namespace+".svc."+clusterDomain+":5432")
	cfg.LookupFunc = d.LookupHost
	cfg.DialFunc = d.DialContext

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer closePgx(t, conn)

	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 returned %d", one)
	}
}

// TestE2EPgxLookupTrap proves pgx's resolve-before-dial trap is real: with only
// DialFunc set, pgconn resolves the in-cluster name with real DNS before the
// dialer runs, which a laptop cannot do, so the connect fails. If this ever
// passes, the README claim is wrong.
func TestE2EPgxLookupTrap(t *testing.T) {
	d := newDialer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	cfg := pgxConfig(t, "pg-0.pg."+namespace+".svc."+clusterDomain+":5432")
	// Deliberately only DialFunc: the trap is that LookupFunc's default wins.
	cfg.DialFunc = d.DialContext

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err == nil {
		closePgx(t, conn)
		t.Fatal("connect succeeded with only DialFunc set; the resolve-before-dial trap is not real")
	}
}

// TestE2EPgxCancelReachesSameBackend proves the RemoteAddr contract. A cancel
// must reach the backend the query is running on, so kubedial reports the pod's
// IP as RemoteAddr and redials it. pgx's default context watcher only sets a
// local deadline and never sends a cancel request; the cancel-request handler
// is the one that makes RemoteAddr load-bearing, so the test installs it. The
// recorded dials show the cancel went to the pod IP rather
// than re-resolving the Service.
func TestE2EPgxCancelReachesSameBackend(t *testing.T) {
	d := newDialer(t)
	ctx := t.Context()

	addr := "pg." + namespace + ".svc:5432"
	ep, ok, err := d.Resolve(ctx, addr)
	if err != nil || !ok {
		t.Fatalf("resolve %s: ok=%v err=%v", addr, ok, err)
	}
	wantCancelAddr := net.JoinHostPort(ep.PodIP, "5432")

	var (
		mu     sync.Mutex
		dialed []string
	)
	dial := func(ctx context.Context, network, a string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, a)
		mu.Unlock()
		return d.DialContext(ctx, network, a)
	}

	cfg := pgxConfig(t, addr)
	cfg.LookupFunc = d.LookupHost
	cfg.DialFunc = dial
	cfg.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		// The deadline is the fallback if the cancel request never lands; it has
		// to outlast the round trip, or the local timeout wins and hides whether
		// the cancel reached the server.
		return &pgconn.CancelRequestContextWatcherHandler{
			Conn:          pgConn,
			DeadlineDelay: 5 * time.Second,
		}
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer closePgx(t, conn)

	qctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	go func() {
		time.Sleep(time.Second)
		cancel()
	}()

	rows, qerr := conn.Query(qctx, "SELECT pg_sleep(30)")
	if qerr == nil {
		for rows.Next() {
		}
		qerr = rows.Err()
		rows.Close()
	}
	elapsed := time.Since(start)

	var pgErr *pgconn.PgError
	if !errors.As(qerr, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("want query_canceled (57014), got %v", qerr)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("cancel took %s, want under 5s", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(dialed, wantCancelAddr) {
		t.Fatalf("cancel request did not dial RemoteAddr %s; dialed %v", wantCancelAddr, dialed)
	}
}

// TestE2EGRPCPassthrough reaches the health service with the passthrough target
// the README prescribes, so no resolution happens before DialGRPC.
func TestE2EGRPCPassthrough(t *testing.T) {
	d := newDialer(t)
	conn, err := grpc.NewClient("passthrough:///grpc."+namespace+".svc:5000",
		grpc.WithContextDialer(d.DialGRPC),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer closeGRPC(t, conn)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health status %s, want SERVING", resp.GetStatus())
	}
}

// TestE2EGRPCDNSTrap proves the passthrough prefix is not optional: without it
// gRPC's default dns resolver runs first, fails on a laptop, and the custom
// dialer is never reached. If this ever succeeds, the README claim is wrong.
func TestE2EGRPCDNSTrap(t *testing.T) {
	d := newDialer(t)
	conn, err := grpc.NewClient("grpc."+namespace+".svc:5000",
		grpc.WithContextDialer(d.DialGRPC),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer closeGRPC(t, conn)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable from the failed dns resolution, got %v", err)
	}
}

// TestE2EPodIP reaches one pod by its status.podIP, the row of the Resolve
// table that a client with no Service in front of it depends on.
func TestE2EPodIP(t *testing.T) {
	d := newDialer(t)
	pod := webPods(t)[0]
	body := getBody(t, d.HTTPClient(nil), "http://"+net.JoinHostPort(pod.Status.PodIP, "8080")+"/hostname")
	if body != pod.Name {
		t.Fatalf("pod IP %s answered as %q, want %q", pod.Status.PodIP, body, pod.Name)
	}
}

// TestE2EReadyCheckRefused asks for a port nothing listens on inside the pod:
// the kubelet accepts the forward and then reports the refusal on the stream's
// error side, which is what Listen turns into ErrNotReady.
func TestE2EReadyCheckRefused(t *testing.T) {
	d := newDialer(t)
	pod := webPods(t)[0]
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ep, ok, err := d.Resolve(ctx, net.JoinHostPort(pod.Status.PodIP, "9999"))
	if err != nil || !ok {
		t.Fatalf("resolve pod IP: ok=%v err=%v", ok, err)
	}
	_, err = d.ListenEndpoint(ctx, ep, kubedial.WithReadyCheck(5*time.Second))
	if !errors.Is(err, kubedial.ErrNotReady) {
		t.Fatalf("want ErrNotReady, got %v", err)
	}
}

// TestE2EReadyCheckDoesNotDelayBind pins the cost of a ready check. A healthy
// forward is silent, so the check always waits out its budget; the probe's own
// teardown must not add the kubelet's error-stream close on top of it. That
// close is about a second on a real cluster, and before this was fixed the bind
// took a budget plus that second.
func TestE2EReadyCheckDoesNotDelayBind(t *testing.T) {
	d := newDialer(t)
	pod := webPods(t)[0]
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ep, ok, err := d.Resolve(ctx, net.JoinHostPort(pod.Status.PodIP, "8080"))
	if err != nil || !ok {
		t.Fatalf("resolve pod IP: ok=%v err=%v", ok, err)
	}

	const budget = 500 * time.Millisecond
	start := time.Now()
	l, err := d.ListenEndpoint(ctx, ep, kubedial.WithReadyCheck(budget))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ListenEndpoint: %v", err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	// A dial on a Kind cluster is tens of milliseconds; the extra ~1s
	// error-stream wait is what this margin has to catch.
	if elapsed > budget+750*time.Millisecond {
		t.Errorf("ready check bound the listener after %s, want about the %s budget", elapsed, budget)
	}
}

// TestE2EDeadlines proves the stream connection carries net.Conn's deadline
// semantics, which is why the data stream is relayed through a net.Pipe: a
// client that bounds a handshake with SetReadDeadline sees a timeout rather
// than a hang.
func TestE2EDeadlines(t *testing.T) {
	d := newDialer(t)
	conn, err := d.DialContext(t.Context(), "tcp", "web."+namespace+".svc:80")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
}

// TestE2EListenNoRedialSurvivesCancel pins the distinction a pinned listener
// has to make. A Postgres cancel request arrives on a second connection to the
// listener, and the pod closes that stream while the cancel connection's client
// is still there. That is the same shape as the pinned pod's whole forward
// ending, but only one stream ended on a live pod, so the listener and the
// session on the first connection must both survive.
//
// The cancel request is sent by hand over a connection this test holds open,
// rather than through pgx's CancelRequest, which closes its connection the
// moment the pod closes the stream and so races the upstream end: the listener
// would see the client leave first and never exercise the shape. Holding the
// cancel connection open makes the upstream end unambiguously the first event.
func TestE2EListenNoRedialSurvivesCancel(t *testing.T) {
	d := newDialer(t)
	ctx := t.Context()

	ep, ok, err := d.Resolve(ctx, "pg-0.pg."+namespace+".svc."+clusterDomain+":5432")
	if err != nil || !ok {
		t.Fatalf("resolve pg-0: ok=%v err=%v", ok, err)
	}
	l, err := d.ListenEndpoint(ctx, ep, kubedial.WithNoRedial())
	if err != nil {
		t.Fatalf("listen endpoint: %v", err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	cfg := pgxConfig(t, l.Addr())
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to listener: %v", err)
	}
	defer closePgx(t, conn)

	// A query long enough that the cancel below reaches it while it is running.
	queryErr := make(chan error, 1)
	go func() {
		_, qerr := conn.Exec(ctx, "SELECT pg_sleep(30)")
		queryErr <- qerr
	}()
	time.Sleep(time.Second)

	// The cancel request: a second connection to the listener, the cancel
	// packet for the session's backend, and then a read that waits for the pod to
	// close the stream. The connection is left open until after the assertions.
	var dialer net.Dialer
	cancelConn, err := dialer.DialContext(ctx, "tcp", l.Addr())
	if err != nil {
		t.Fatalf("dial cancel connection: %v", err)
	}
	defer func() {
		if err := cancelConn.Close(); err != nil {
			t.Errorf("close cancel connection: %v", err)
		}
	}()
	if _, err := cancelConn.Write(cancelRequestPacket(conn.PgConn())); err != nil {
		t.Fatalf("write cancel request: %v", err)
	}
	if _, err := cancelConn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("cancel connection read = %v, want io.EOF from the pod closing it", err)
	}

	select {
	case qerr := <-queryErr:
		var pgErr *pgconn.PgError
		if !errors.As(qerr, &pgErr) || pgErr.Code != "57014" {
			t.Fatalf("want query_canceled (57014) through the listener, got %v", qerr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("cancelled query did not return")
	}

	// The cancel ended one stream; the listener must still be running.
	if err := l.Err(); err != nil {
		t.Fatalf("listener stopped after a cancel request: %v", err)
	}
	select {
	case <-l.Done():
		t.Fatal("listener stopped after a cancel request")
	default:
	}

	// And the session on the original connection is still usable.
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("query on the original session after a cancel: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 after a cancel returned %d", one)
	}
}

// cancelRequestPacket builds the libpq cancel request for pgConn's backend: its
// length, the cancel-request code, the backend PID and the secret key. It is
// the packet pgconn.CancelRequest sends.
func cancelRequestPacket(pgConn *pgconn.PgConn) []byte {
	secret := pgConn.SecretKey()
	buf := make([]byte, 12+len(secret))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	binary.BigEndian.PutUint32(buf[4:8], 80877102)
	binary.BigEndian.PutUint32(buf[8:12], pgConn.PID())
	copy(buf[12:], secret)
	return buf
}

// TestE2EListenPinnedNoRedial pins a listener to one StatefulSet ordinal and
// deletes that pod: WithNoRedial has to stop the listener with an error rather
// than redial a pod the caller did not ask for. It runs last because it destroys
// a fixture the other tests use.
func TestE2EListenPinnedNoRedial(t *testing.T) {
	d := newDialer(t)
	ctx := t.Context()

	ep, ok, err := d.Resolve(ctx, "pg-0.pg."+namespace+".svc."+clusterDomain+":5432")
	if err != nil || !ok {
		t.Fatalf("resolve pg-0: ok=%v err=%v", ok, err)
	}
	l, err := d.ListenEndpoint(ctx, ep, kubedial.WithNoRedial())
	if err != nil {
		t.Fatalf("listen endpoint: %v", err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	// A psql-style session: an ordinary client against the loopback address,
	// idle while the pod goes away.
	cfg := pgxConfig(t, l.Addr())
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to listener: %v", err)
	}
	defer closePgx(t, conn)

	if _, err := kubectl(ctx, "delete", "pod", ep.Pod, "-n", namespace); err != nil {
		t.Fatalf("delete pod: %v", err)
	}

	select {
	case <-l.Done():
	case <-time.After(60 * time.Second):
		t.Fatal("listener did not stop within 60s of the pinned pod being deleted")
	}
	if err := l.Err(); err == nil {
		t.Fatal("listener stopped with a nil error; the pinned target's loss was not reported")
	}
}

// newDialer builds a Dialer from the Kind kubeconfig. The mode must settle to
// forward: these tests run on a laptop, and a direct dial would reach nothing.
func newDialer(t *testing.T) *kubedial.Dialer {
	t.Helper()
	cfg, err := kindRESTConfig()
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}
	d, err := kubedial.New(cfg, kubedial.WithNamespace(namespace))
	if err != nil {
		t.Fatalf("new dialer: %v", err)
	}
	if got := d.Mode(); got != kubedial.ModeForward {
		t.Fatalf("dialer mode %s, want forward outside a cluster", got)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close dialer: %v", err)
		}
	})
	return d
}

func kindRESTConfig() (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeContext}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
}

func newClientset(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	cfg, err := kindRESTConfig()
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("clientset: %v", err)
	}
	return cs
}

func webPods(t *testing.T) []corev1.Pod {
	t.Helper()
	list, err := newClientset(t).CoreV1().Pods(namespace).List(t.Context(), metav1.ListOptions{LabelSelector: "app=web"})
	if err != nil {
		t.Fatalf("list web pods: %v", err)
	}
	if len(list.Items) < 2 {
		t.Fatalf("want at least 2 web pods, got %d", len(list.Items))
	}
	return list.Items
}

func webPodNames(t *testing.T) map[string]bool {
	t.Helper()
	names := make(map[string]bool)
	for _, pod := range webPods(t) {
		names[pod.Name] = true
	}
	return names
}

func getBody(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %s, body %q", url, resp.Status, body)
	}
	return strings.TrimSpace(string(body))
}

func pgxConfig(t *testing.T, addr string) *pgx.ConnConfig {
	t.Helper()
	cfg, err := pgx.ParseConfig("postgres://postgres:e2e@" + addr + "/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("parse conn string for %s: %v", addr, err)
	}
	return cfg
}

func closePgx(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if err := conn.Close(context.Background()); err != nil {
		t.Errorf("close pgx conn: %v", err)
	}
}

func closeGRPC(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	if err := conn.Close(); err != nil {
		t.Errorf("close grpc conn: %v", err)
	}
}
