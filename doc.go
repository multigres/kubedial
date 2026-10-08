// Package kubedial dials Kubernetes in-cluster addresses from outside a cluster.
//
// The same address that resolves inside a cluster ("my-svc", "db.default.svc",
// "db-0.headless.default.svc.cluster.local", a pod IP) normally fails from a
// laptop. kubedial resolves such an address to a ready pod and opens a
// Kubernetes port-forward stream to that pod, handing the stream back as a
// net.Conn; when the process is already in-cluster it dials the address
// directly instead.
//
// It is built in layers, each on the one below:
//
//   - Resolve maps an address to the pod and port it names.
//   - Dial opens a net.Conn to that pod, over a port-forward stream out of
//     cluster or directly inside one. This is the layer most clients use, by
//     giving it to their own dial hook.
//   - net/http builds an *http.Transport and *http.Client on that dialer, and
//     Do retries a request when the connection under it died.
//   - Listen accepts on a stable local address and hands each connection to
//     Dial, for clients that can only be given an address, such as psql or a
//     child process.
//
// The package holds no global state, does not read configuration of its own,
// and prints nothing. Callers observe what happened through returned errors and
// the hooks they set.
package kubedial
