package kubedial

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceGetter reads one Service. It is satisfied by
// kubernetes.Interface.CoreV1().Services(ns).
type ServiceGetter interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Service, error)
}

// PodLister lists Pods in a namespace. It is satisfied by
// kubernetes.Interface.CoreV1().Pods(ns).
type PodLister interface {
	List(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error)
}

// EndpointSliceLister lists EndpointSlices in a namespace. It is satisfied by
// kubernetes.Interface.DiscoveryV1().EndpointSlices(ns).
type EndpointSliceLister interface {
	List(ctx context.Context, opts metav1.ListOptions) (*discoveryv1.EndpointSliceList, error)
}

// clients is the namespace-scoped surface lookup needs, built per namespace.
type clients interface {
	Services(ns string) ServiceGetter
	Pods(ns string) PodLister
	EndpointSlices(ns string) EndpointSliceLister
}

// lookup resolves a parsed address to the pod and port it names, deterministically:
// the first candidate lookupAll returns. It returns ok=false with a nil error for
// an address that is not in-cluster, so the caller can fall through to real DNS.
func lookup(ctx context.Context, c clients, p parsedAddr, defaultNS string) (ep Endpoint, ok bool, err error) {
	eps, ok, err := lookupAll(ctx, c, p, defaultNS)
	if err != nil || !ok {
		return Endpoint{}, ok, err
	}
	return eps[0], true, nil
}

// lookupAll resolves a parsed address to every ready candidate for it, in the
// order lookup picks from. A Service can have several ready endpoints; the
// headless-pod and pod-IP kinds have at most one, so the caller's spread picker
// is a no-op for them.
func lookupAll(ctx context.Context, c clients, p parsedAddr, defaultNS string) ([]Endpoint, bool, error) {
	switch p.Kind {
	case kindService:
		return lookupService(ctx, c, p, defaultNS)
	case kindHeadlessPod:
		return lookupHeadlessPod(ctx, c, p, defaultNS)
	case kindPodIP:
		return lookupPodIP(ctx, c, p, defaultNS)
	default:
		return nil, false, nil
	}
}

func lookupService(ctx context.Context, c clients, p parsedAddr, defaultNS string) ([]Endpoint, bool, error) {
	ns := p.Namespace
	if ns == "" {
		ns = defaultNS
	}
	svc, err := c.Services(ns).Get(ctx, p.Service, metav1.GetOptions{})
	if err != nil {
		switch {
		case apierrors.IsNotFound(err):
			if !p.Qualified {
				// An unqualified name is in-cluster only if the Service exists;
				// otherwise it is an ordinary host name for real DNS.
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("kubedial: service %s/%s: %w", ns, p.Service, ErrNotFound)
		case apierrors.IsForbidden(err) && !p.Qualified:
			// Without cluster-wide get on services we cannot establish that an
			// unqualified name is a Service at all, so it is dialled directly,
			// exactly as a NotFound is. A qualified name is asserted to be a
			// Service, so the refusal stays an error.
			return nil, false, nil
		default:
			return nil, false, fmt.Errorf("kubedial: get service %s/%s: %w", ns, p.Service, err)
		}
	}
	if svc.Spec.Type == corev1.ServiceTypeExternalName {
		return nil, false, fmt.Errorf("kubedial: service %s/%s: %w", ns, p.Service, ErrUnsupportedService)
	}

	slices, err := serviceSlices(ctx, c.EndpointSlices(ns), ns, p.Service)
	if err != nil {
		return nil, false, err
	}

	var refs []endpointRef
	if svc.Spec.ClusterIP == corev1.ClusterIPNone {
		// A headless Service publishes the pod port, so the address port is
		// the target as it stands.
		refs = readyEndpointRefs(slices, p.Port)
	} else {
		svcPort, found := servicePortByNumber(svc, p.Port)
		if !found {
			return nil, false, fmt.Errorf("kubedial: service %s/%s has no port %d: %w", ns, p.Service, p.Port, ErrPortNotFound)
		}
		var portFound bool
		refs, portFound = readyEndpointRefsByPortName(slices, svcPort.Name)
		if !portFound {
			return nil, false, fmt.Errorf("kubedial: service %s/%s port %q has no endpoint port: %w", ns, p.Service, svcPort.Name, ErrPortNotFound)
		}
	}
	if len(refs) == 0 {
		return nil, false, fmt.Errorf("kubedial: service %s/%s: %w", ns, p.Service, ErrNoReadyEndpoint)
	}
	// First by pod name and address: a stable order so lookup is deterministic
	// and the caller's spread picker has a defined index space.
	return endpointRefsToEndpoints(refs, ns), true, nil
}

// endpointRefsToEndpoints turns ready refs into candidate endpoints in order.
func endpointRefsToEndpoints(refs []endpointRef, ns string) []Endpoint {
	eps := make([]Endpoint, len(refs))
	for i, ref := range refs {
		eps[i] = Endpoint{Namespace: ns, Pod: ref.pod, PodIP: ref.ip, Port: ref.port}
	}
	return eps
}

func lookupHeadlessPod(ctx context.Context, c clients, p parsedAddr, defaultNS string) ([]Endpoint, bool, error) {
	ns := p.Namespace
	if ns == "" {
		ns = defaultNS
	}
	slices, err := serviceSlices(ctx, c.EndpointSlices(ns), ns, p.Service)
	if err != nil {
		return nil, false, err
	}
	ref, found := headlessEndpointRef(slices, p.Host, p.Port)
	if !found {
		return nil, false, fmt.Errorf("kubedial: no endpoint for %s.%s/%s: %w", p.Host, p.Service, ns, ErrNotFound)
	}
	return []Endpoint{{Namespace: ns, Pod: ref.pod, PodIP: ref.ip, Port: ref.port}}, true, nil
}

func lookupPodIP(ctx context.Context, c clients, p parsedAddr, defaultNS string) ([]Endpoint, bool, error) {
	opts := metav1.ListOptions{FieldSelector: "status.podIP=" + p.Host}
	pods, err := c.Pods("").List(ctx, opts)
	if apierrors.IsForbidden(err) {
		// A cluster-wide pod list needs cluster-scoped RBAC; the default
		// namespace is the useful fallback for a namespace-scoped client.
		pods, err = c.Pods(defaultNS).List(ctx, opts)
	}
	if err != nil {
		return nil, false, fmt.Errorf("kubedial: list pods with IP %s: %w", p.Host, err)
	}

	candidates := usablePods(pods.Items)
	switch len(candidates) {
	case 0:
		// An IP that matches no pod is not in-cluster: it may be a node, the
		// API server, or an external host, all of which the caller dials
		// directly.
		return nil, false, nil
	case 1:
		pod := candidates[0]
		return []Endpoint{{Namespace: pod.Namespace, Pod: pod.Name, PodIP: pod.Status.PodIP, Port: p.Port}}, true, nil
	default:
		return nil, false, fmt.Errorf("kubedial: pod IP %s: %w", p.Host, ErrAmbiguousPodIP)
	}
}

// ResolveService finds the pod behind a Service and maps the named Service
// port onto that pod's port, using the Service's EndpointSlices.
//
// It is the explicit-target entry point: the caller already knows which
// Service and port name it wants, unlike lookup, which starts from a dialled
// address. It picks the same endpoint lookup would, so both agree on the pod
// for a Service and neither needs get on pods.
func ResolveService(ctx context.Context, svcs ServiceGetter, slices EndpointSliceLister, namespace, name, port string) (Endpoint, error) {
	svc, err := svcs.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Endpoint{}, fmt.Errorf("kubedial: service %s/%s: %w", namespace, name, ErrNotFound)
	}
	if err != nil {
		return Endpoint{}, fmt.Errorf("kubedial: get service %s/%s: %w", namespace, name, err)
	}
	if svc.Spec.Type == corev1.ServiceTypeExternalName {
		return Endpoint{}, fmt.Errorf("kubedial: service %s/%s: %w", namespace, name, ErrUnsupportedService)
	}

	svcPort, found := servicePortByName(svc, port)
	if !found {
		return Endpoint{}, fmt.Errorf("kubedial: service %s/%s has no port %q: %w", namespace, name, port, ErrPortNotFound)
	}
	items, err := serviceSlices(ctx, slices, namespace, name)
	if err != nil {
		return Endpoint{}, err
	}
	refs, portFound := readyEndpointRefsByPortName(items, svcPort.Name)
	if !portFound {
		return Endpoint{}, fmt.Errorf("kubedial: service %s/%s port %q has no endpoint port: %w", namespace, name, port, ErrPortNotFound)
	}
	if len(refs) == 0 {
		return Endpoint{}, fmt.Errorf("kubedial: service %s/%s: %w", namespace, name, ErrNoReadyEndpoint)
	}
	ref := refs[0]
	return Endpoint{Namespace: namespace, Pod: ref.pod, PodIP: ref.ip, Port: ref.port}, nil
}

// ResolvePod finds the pod matching selector in namespace and forwards to
// port directly, with no Service in between.
func ResolvePod(ctx context.Context, pods PodLister, namespace, selector string, port int32) (Endpoint, error) {
	list, err := pods.List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return Endpoint{}, fmt.Errorf("kubedial: list pods matching %q in %s: %w", selector, namespace, err)
	}
	if len(list.Items) == 0 {
		return Endpoint{}, fmt.Errorf("kubedial: no pod matching %q in %s: %w", selector, namespace, ErrNotFound)
	}
	best := bestPod(podPointers(list.Items))
	if !podReady(best) {
		return Endpoint{}, fmt.Errorf("kubedial: pod matching %q in %s: %w", selector, namespace, ErrNoReadyEndpoint)
	}
	return Endpoint{Namespace: best.Namespace, Pod: best.Name, PodIP: best.Status.PodIP, Port: port}, nil
}

// endpointRef is one ready, pod-backed EndpointSlice endpoint reduced to what
// an Endpoint needs, with the target port resolved from that endpoint's own
// slice.
type endpointRef struct {
	pod  string
	ip   string
	port int32
}

func serviceSlices(ctx context.Context, slices EndpointSliceLister, namespace, svc string) ([]discoveryv1.EndpointSlice, error) {
	list, err := slices.List(ctx, metav1.ListOptions{
		LabelSelector: "kubernetes.io/service-name=" + svc,
	})
	if err != nil {
		return nil, fmt.Errorf("kubedial: list endpointslices for service %s/%s: %w", namespace, svc, err)
	}
	return list.Items, nil
}

// readyEndpointRefs returns the ready, pod-backed endpoints with port set to
// port, which a headless Service or a pod form uses as is.
func readyEndpointRefs(slices []discoveryv1.EndpointSlice, port int32) []endpointRef {
	refs := make([]endpointRef, 0, len(slices))
	for si := range slices {
		for ei := range slices[si].Endpoints {
			ep := &slices[si].Endpoints[ei]
			if !endpointReady(ep) || !podBacked(ep) {
				continue
			}
			refs = append(refs, endpointRef{pod: ep.TargetRef.Name, ip: firstAddress(ep), port: port})
		}
	}
	sortEndpointRefs(refs)
	return refs
}

// readyEndpointRefsByPortName resolves the target port per slice, because the
// EndpointSlice controller puts pods whose named port resolves differently
// into different slices; a ref is kept only when its own slice carries
// portName. portFound separates "no slice carries the port" from "no ready
// endpoint", which are different errors.
func readyEndpointRefsByPortName(slices []discoveryv1.EndpointSlice, portName string) (refs []endpointRef, portFound bool) {
	refs = make([]endpointRef, 0, len(slices))
	for si := range slices {
		port, ok := slicePort(&slices[si], portName)
		if !ok {
			continue
		}
		portFound = true
		for ei := range slices[si].Endpoints {
			ep := &slices[si].Endpoints[ei]
			if !endpointReady(ep) || !podBacked(ep) {
				continue
			}
			refs = append(refs, endpointRef{pod: ep.TargetRef.Name, ip: firstAddress(ep), port: port})
		}
	}
	sortEndpointRefs(refs)
	return refs, portFound
}

// sortEndpointRefs orders refs by pod name and then address, so that a
// dual-stack cluster, which lists one endpoint per address family, cannot flip
// the chosen PodIP between runs.
func sortEndpointRefs(refs []endpointRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].pod != refs[j].pod {
			return refs[i].pod < refs[j].pod
		}
		return refs[i].ip < refs[j].ip
	})
}

// headlessEndpointRef matches a pod-backed endpoint by hostname, falling back
// to the pod name, which is how cluster DNS serves a headless Service.
func headlessEndpointRef(slices []discoveryv1.EndpointSlice, host string, port int32) (endpointRef, bool) {
	var fallback *endpointRef
	for si := range slices {
		for ei := range slices[si].Endpoints {
			ep := &slices[si].Endpoints[ei]
			if !podBacked(ep) {
				continue
			}
			if ep.Hostname != nil && *ep.Hostname == host {
				return endpointRef{pod: ep.TargetRef.Name, ip: firstAddress(ep), port: port}, true
			}
			if fallback == nil && ep.TargetRef.Name == host {
				ref := endpointRef{pod: ep.TargetRef.Name, ip: firstAddress(ep), port: port}
				fallback = &ref
			}
		}
	}
	if fallback != nil {
		return *fallback, true
	}
	return endpointRef{}, false
}

// bestPod picks the pod a forward should go to. activePodsLess puts the pod a
// controller would delete first at the front, so sorting by it reversed puts
// the best candidate first; pods are pre-sorted by name so ties are stable.
func bestPod(pods []*corev1.Pod) *corev1.Pod {
	sorted := make([]*corev1.Pod, len(pods))
	copy(sorted, pods)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Name != sorted[j].Name {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Namespace < sorted[j].Namespace
	})
	sort.SliceStable(sorted, func(i, j int) bool { return activePodsLess(sorted[j], sorted[i]) })
	return sorted[0]
}

func podPointers(pods []corev1.Pod) []*corev1.Pod {
	ptrs := make([]*corev1.Pod, len(pods))
	for i := range pods {
		ptrs[i] = &pods[i]
	}
	return ptrs
}

// usablePods drops finished pods, whose IP the cluster has already freed for
// reuse, and host-network pods, whose IP belongs to the node rather than the
// pod, unless they are the only pods that matched.
func usablePods(pods []corev1.Pod) []*corev1.Pod {
	matches := make([]*corev1.Pod, 0, len(pods))
	hostNetwork := make([]*corev1.Pod, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.Spec.HostNetwork {
			hostNetwork = append(hostNetwork, pod)
			continue
		}
		matches = append(matches, pod)
	}
	if len(matches) > 0 {
		return matches
	}
	return hostNetwork
}

func servicePortByNumber(svc *corev1.Service, port int32) (*corev1.ServicePort, bool) {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Port == port {
			return &svc.Spec.Ports[i], true
		}
	}
	return nil, false
}

func servicePortByName(svc *corev1.Service, name string) (*corev1.ServicePort, bool) {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Name == name {
			return &svc.Spec.Ports[i], true
		}
	}
	return nil, false
}

// slicePort returns the target port of one slice whose name matches the
// Service port name; an unnamed Service port matches an unnamed slice port.
func slicePort(slice *discoveryv1.EndpointSlice, name string) (int32, bool) {
	for pi := range slice.Ports {
		p := &slice.Ports[pi]
		if p.Port == nil {
			continue
		}
		if stringOrEmpty(p.Name) == name {
			return *p.Port, true
		}
	}
	return 0, false
}

// endpointReady reports whether an endpoint is ready. A nil ready condition
// means ready, per the EndpointSlice API.
func endpointReady(ep *discoveryv1.Endpoint) bool {
	return ep.Conditions.Ready == nil || *ep.Conditions.Ready
}

func podBacked(ep *discoveryv1.Endpoint) bool {
	return ep.TargetRef != nil && ep.TargetRef.Kind == "Pod"
}

func firstAddress(ep *discoveryv1.Endpoint) string {
	if len(ep.Addresses) == 0 {
		return ""
	}
	return ep.Addresses[0]
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
