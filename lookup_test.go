package kubedial

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// fakeClients adapts the fake clientset to clients. The typed fake applies
// label selectors but not field selectors (client-go gentype/fake.go), so
// fakePods emulates the field selector here; without that a pod-IP lookup
// would see every pod in the namespace.
type fakeClients struct {
	cs             *fake.Clientset
	forbid         map[string]bool
	forbidServices bool
}

func newFakeClients(objs ...runtime.Object) *fakeClients {
	return &fakeClients{cs: fake.NewSimpleClientset(objs...), forbid: map[string]bool{}}
}

func (f *fakeClients) Services(ns string) ServiceGetter {
	return fakeServices{cs: f.cs, ns: ns, forbidden: f.forbidServices}
}

func (f *fakeClients) Pods(ns string) PodLister {
	return fakePods{cs: f.cs, ns: ns, forbidden: f.forbid[ns]}
}

func (f *fakeClients) EndpointSlices(ns string) EndpointSliceLister {
	return fakeSlices{cs: f.cs, ns: ns}
}

type fakePods struct {
	cs        *fake.Clientset
	ns        string
	forbidden bool
}
type fakeServices struct {
	cs        *fake.Clientset
	ns        string
	forbidden bool
}

func (s fakeServices) Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Service, error) {
	if s.forbidden {
		return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, name, errors.New("forbidden"))
	}
	return s.cs.CoreV1().Services(s.ns).Get(ctx, name, opts)
}

func (p fakePods) List(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
	if p.forbidden {
		return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("forbidden"))
	}
	list, err := p.cs.CoreV1().Pods(p.ns).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	return filterPodsByIP(list, opts.FieldSelector)
}

type fakeSlices struct {
	cs *fake.Clientset
	ns string
}

func (s fakeSlices) List(ctx context.Context, opts metav1.ListOptions) (*discoveryv1.EndpointSliceList, error) {
	return s.cs.DiscoveryV1().EndpointSlices(s.ns).List(ctx, opts)
}

func filterPodsByIP(list *corev1.PodList, selector string) (*corev1.PodList, error) {
	if selector == "" {
		return list, nil
	}
	sel, err := fields.ParseSelector(selector)
	if err != nil {
		return nil, err
	}
	out := &corev1.PodList{}
	for i := range list.Items {
		pod := &list.Items[i]
		if sel.Matches(fields.Set{"status.podIP": pod.Status.PodIP}) {
			out.Items = append(out.Items, *pod)
		}
	}
	return out, nil
}

func TestLookupServiceNotFoundUnqualifiedFallsThrough(t *testing.T) {
	c := newFakeClients()
	p := parsedAddr{Kind: kindService, Service: "missing", Port: 80}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false (falls through to DNS)")
	}
	if ep != (Endpoint{}) {
		t.Fatalf("endpoint = %+v, want zero", ep)
	}
}

func TestLookupServiceNotFoundQualifiedErrors(t *testing.T) {
	c := newFakeClients()
	p := parsedAddr{Kind: kindService, Service: "missing", Namespace: "ns", Port: 80, Qualified: true}

	_, ok, err := lookup(t.Context(), c, p, "default")
	if ok {
		t.Fatalf("ok = true, want false")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestLookupExternalNameRefused(t *testing.T) {
	svc := newService("ns", "ext", "")
	svc.Spec.Type = corev1.ServiceTypeExternalName
	svc.Spec.ExternalName = "db.example.com"
	c := newFakeClients(svc)
	p := parsedAddr{Kind: kindService, Service: "ext", Namespace: "ns", Port: 80}

	_, _, err := lookup(t.Context(), c, p, "default")
	if !errors.Is(err, ErrUnsupportedService) {
		t.Fatalf("err = %v, want ErrUnsupportedService", err)
	}
}

func TestLookupClusterIPMapsServicePortByName(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{
		{Name: "metrics", Port: 9090, TargetPort: intstrFromInt(9091)},
		{Name: "http", Port: 80, TargetPort: intstrFromInt(8080)},
	}
	// The decoy port is listed first, so taking Ports[0] instead of matching
	// the Service port name by name picks 9091.
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{
			{Name: strPtr("metrics"), Port: int32Ptr(9091)},
			{Name: strPtr("http"), Port: int32Ptr(8080)},
		},
		endpoint("web-1", "10.1.0.5", nil),
	)
	c := newFakeClients(svc, slice)
	p := parsedAddr{Kind: kindService, Service: "web", Namespace: "ns", Port: 80}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	want := Endpoint{Namespace: "ns", Pod: "web-1", PodIP: "10.1.0.5", Port: 8080}
	if ep != want {
		t.Fatalf("endpoint = %+v, want %+v", ep, want)
	}
}

func TestLookupClusterIPMapsPortPerSlice(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstrFromInt(8080)}}
	// A rollout that changes the named containerPort's number splits the pods
	// across slices, each carrying its own resolved port. Pod "a" sorts first
	// by name, so it must take 9090 from its own slice, not 8080 from "b"'s.
	c := newFakeClients(svc,
		newSliceNamed("ns", "web", "a-slice",
			[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
			endpoint("b", "10.1.0.2", nil),
		),
		newSliceNamed("ns", "web", "z-slice",
			[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(9090)}},
			endpoint("a", "10.1.0.1", nil),
		),
	)
	p := parsedAddr{Kind: kindService, Service: "web", Namespace: "ns", Port: 80}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	want := Endpoint{Namespace: "ns", Pod: "a", PodIP: "10.1.0.1", Port: 9090}
	if ep != want {
		t.Fatalf("endpoint = %+v, want %+v", ep, want)
	}
}

func TestLookupClusterIPUnknownPort(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
	c := newFakeClients(svc)
	p := parsedAddr{Kind: kindService, Service: "web", Namespace: "ns", Port: 9999}

	_, _, err := lookup(t.Context(), c, p, "default")
	if !errors.Is(err, ErrPortNotFound) {
		t.Fatalf("err = %v, want ErrPortNotFound", err)
	}
}

func TestLookupHeadlessUsesPortAsIs(t *testing.T) {
	svc := newService("ns", "pg", corev1.ClusterIPNone)
	svc.Spec.Ports = []corev1.ServicePort{{Name: "pg", Port: 5432, TargetPort: intstrFromInt(6432)}}
	slice := newSlice("ns", "pg",
		[]discoveryv1.EndpointPort{{Name: strPtr("pg"), Port: int32Ptr(6432)}},
		endpoint("pg-0", "10.1.0.7", nil),
	)
	c := newFakeClients(svc, slice)
	p := parsedAddr{Kind: kindService, Service: "pg", Namespace: "ns", Port: 5432}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.Port != 5432 {
		t.Fatalf("port = %d, want 5432 (used as is)", ep.Port)
	}
}

func TestLookupSkipsNotReadyEndpoints(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
		endpoint("a", "10.1.0.1", boolPtr(false)),
		endpoint("b", "10.1.0.2", boolPtr(true)),
	)
	c := newFakeClients(svc, slice)
	p := parsedAddr{Kind: kindService, Service: "web", Namespace: "ns", Port: 80}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.Pod != "b" {
		t.Fatalf("pod = %q, want the ready endpoint %q", ep.Pod, "b")
	}
}

func TestLookupNilReadyCountsAsReady(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
		endpoint("only", "10.1.0.3", nil),
	)
	c := newFakeClients(svc, slice)
	p := parsedAddr{Kind: kindService, Service: "web", Namespace: "ns", Port: 80}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.Pod != "only" {
		t.Fatalf("pod = %q, want %q", ep.Pod, "only")
	}
}

func TestLookupHeadlessPodByHostname(t *testing.T) {
	slice := newSlice("ns", "db",
		[]discoveryv1.EndpointPort{{Name: strPtr("pg"), Port: int32Ptr(5432)}},
		// The targetRef name deliberately differs from the hostname, so a match
		// can only come from the hostname field.
		hostnameEndpoint("db-0", "10.1.0.8", "db-0-pod"),
	)
	c := newFakeClients(slice)
	p := parsedAddr{Kind: kindHeadlessPod, Host: "db-0", Service: "db", Namespace: "ns", Port: 5432, Qualified: true}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	want := Endpoint{Namespace: "ns", Pod: "db-0-pod", PodIP: "10.1.0.8", Port: 5432}
	if ep != want {
		t.Fatalf("endpoint = %+v, want %+v", ep, want)
	}
}

func TestLookupHeadlessPodByTargetRefName(t *testing.T) {
	slice := newSlice("ns", "db",
		[]discoveryv1.EndpointPort{{Name: strPtr("pg"), Port: int32Ptr(5432)}},
		endpoint("db-1", "10.1.0.9", nil),
	)
	c := newFakeClients(slice)
	p := parsedAddr{Kind: kindHeadlessPod, Host: "db-1", Service: "db", Namespace: "ns", Port: 5432, Qualified: true}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.Pod != "db-1" {
		t.Fatalf("pod = %q, want %q", ep.Pod, "db-1")
	}
}

func TestLookupPodIPForbiddenFallsBackToDefaultNamespace(t *testing.T) {
	// The decoy has another IP, so a lookup that dropped the field selector
	// would see two pods and fail as ambiguous.
	c := newFakeClients(pod("default", "app-1", "10.1.0.9"), pod("default", "decoy", "10.9.9.9"))
	c.forbid[""] = true
	p := parsedAddr{Kind: kindPodIP, Host: "10.1.0.9", Port: 8080}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	want := Endpoint{Namespace: "default", Pod: "app-1", PodIP: "10.1.0.9", Port: 8080}
	if ep != want {
		t.Fatalf("endpoint = %+v, want %+v", ep, want)
	}
}

func TestLookupPodIPIgnoresHostNetworkWhenOthersMatch(t *testing.T) {
	hostNet := pod("ns", "hostnet", "10.1.0.9")
	hostNet.Spec.HostNetwork = true
	c := newFakeClients(hostNet, pod("ns", "normal", "10.1.0.9"), pod("ns", "decoy", "10.9.9.9"))
	p := parsedAddr{Kind: kindPodIP, Host: "10.1.0.9", Port: 8080}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.Pod != "normal" {
		t.Fatalf("pod = %q, want %q", ep.Pod, "normal")
	}
}

func TestLookupPodIPAmbiguous(t *testing.T) {
	c := newFakeClients(pod("ns", "a", "10.1.0.9"), pod("ns", "b", "10.1.0.9"), pod("ns", "decoy", "10.9.9.9"))
	p := parsedAddr{Kind: kindPodIP, Host: "10.1.0.9", Port: 8080}

	_, _, err := lookup(t.Context(), c, p, "default")
	if !errors.Is(err, ErrAmbiguousPodIP) {
		t.Fatalf("err = %v, want ErrAmbiguousPodIP", err)
	}
}

func TestLookupSetsPodIP(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
		endpoint("web-1", "10.2.3.4", nil),
	)
	c := newFakeClients(svc, slice)
	p := parsedAddr{Kind: kindService, Service: "web", Namespace: "ns", Port: 80}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.PodIP != "10.2.3.4" {
		t.Fatalf("podIP = %q, want %q", ep.PodIP, "10.2.3.4")
	}
}

// TestSortEndpointRefsDualStackTieBreak pins N7: a dual-stack pod appears once
// per address family, so the pod-name keys tie and the address string must
// decide, not slice order, or the reported RemoteAddr would flip between runs.
func TestSortEndpointRefsDualStackTieBreak(t *testing.T) {
	refs := []endpointRef{
		{pod: "web-1", ip: "2001:db8::5", port: 8080},
		{pod: "web-1", ip: "10.1.0.5", port: 8080},
	}
	sortEndpointRefs(refs)
	if refs[0].ip != "10.1.0.5" {
		t.Fatalf("first ref ip = %q, want %q (string order ties the pod name)", refs[0].ip, "10.1.0.5")
	}
}

func TestResolveServiceNamedPort(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstrFromInt(8080)}}
	slice := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{{Name: strPtr("http"), Port: int32Ptr(8080)}},
		endpoint("web-1", "10.1.0.5", nil),
	)
	c := newFakeClients(svc, slice)

	ep, err := ResolveService(t.Context(),
		c.cs.CoreV1().Services("ns"),
		c.cs.DiscoveryV1().EndpointSlices("ns"),
		"ns", "web", "http")
	if err != nil {
		t.Fatalf("ResolveService: %v", err)
	}
	want := Endpoint{Namespace: "ns", Pod: "web-1", PodIP: "10.1.0.5", Port: 8080}
	if ep != want {
		t.Fatalf("endpoint = %+v, want %+v", ep, want)
	}
}

func TestResolvePodPrefersReady(t *testing.T) {
	notReady := pod("ns", "web-0", "10.1.0.1")
	notReady.Labels = map[string]string{"app": "web"}
	rea := readyPod("ns", "web-1", "10.1.0.2")
	rea.Labels = map[string]string{"app": "web"}
	c := newFakeClients(notReady, rea)

	ep, err := ResolvePod(t.Context(), c.cs.CoreV1().Pods("ns"), "ns", "app=web", 8080)
	if err != nil {
		t.Fatalf("ResolvePod: %v", err)
	}
	if ep.Pod != "web-1" {
		t.Fatalf("pod = %q, want the ready pod %q", ep.Pod, "web-1")
	}
	if ep.PodIP != "10.1.0.2" {
		t.Fatalf("podIP = %q, want %q", ep.PodIP, "10.1.0.2")
	}
}

func TestLookupPodIPSkipsFinishedPods(t *testing.T) {
	done := pod("ns", "job-1", "10.1.0.9")
	done.Status.Phase = corev1.PodSucceeded
	c := newFakeClients(done, pod("ns", "live", "10.1.0.9"))
	p := parsedAddr{Kind: kindPodIP, Host: "10.1.0.9", Port: 8080}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.Pod != "live" {
		t.Fatalf("pod = %q, want %q", ep.Pod, "live")
	}
}

func TestLookupPodIPFinishedPodFallsThrough(t *testing.T) {
	done := pod("ns", "job-1", "10.1.0.9")
	done.Status.Phase = corev1.PodFailed
	c := newFakeClients(done)
	p := parsedAddr{Kind: kindPodIP, Host: "10.1.0.9", Port: 8080}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false (not in-cluster)")
	}
	if ep != (Endpoint{}) {
		t.Fatalf("endpoint = %+v, want zero", ep)
	}
}

func TestLookupPodIPUnknownFallsThrough(t *testing.T) {
	// A node IP, the API server's IP or an external host matches no pod, and
	// the caller must dial it directly rather than get an error.
	c := newFakeClients(pod("ns", "other", "10.9.9.9"))
	p := parsedAddr{Kind: kindPodIP, Host: "10.1.0.9", Port: 8080}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false (dial directly)")
	}
	if ep != (Endpoint{}) {
		t.Fatalf("endpoint = %+v, want zero", ep)
	}
}

func TestLookupPodIPForbiddenFallbackEmptyFallsThrough(t *testing.T) {
	c := newFakeClients()
	c.forbid[""] = true
	p := parsedAddr{Kind: kindPodIP, Host: "10.1.0.9", Port: 8080}

	_, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false")
	}
}

func TestLookupForbiddenUnqualifiedFallsThrough(t *testing.T) {
	c := newFakeClients()
	c.forbidServices = true
	p := parsedAddr{Kind: kindService, Service: "example", Namespace: "com", Port: 443}

	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false (dial directly)")
	}
	if ep != (Endpoint{}) {
		t.Fatalf("endpoint = %+v, want zero", ep)
	}
}

func TestLookupForbiddenQualifiedErrors(t *testing.T) {
	c := newFakeClients()
	c.forbidServices = true
	p := parsedAddr{Kind: kindService, Service: "web", Namespace: "ns", Port: 80, Qualified: true}

	_, ok, err := lookup(t.Context(), c, p, "default")
	if ok {
		t.Fatalf("ok = true, want false")
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("err = %v, want a Forbidden error", err)
	}
}

func TestLookupHeadlessPodHostnameWithoutTargetRef(t *testing.T) {
	slice := newSlice("ns", "db",
		[]discoveryv1.EndpointPort{{Name: strPtr("pg"), Port: int32Ptr(5432)}},
		discoveryv1.Endpoint{Addresses: []string{"10.1.0.8"}, Hostname: strPtr("db-0")},
	)
	c := newFakeClients(slice)
	p := parsedAddr{Kind: kindHeadlessPod, Host: "db-0", Service: "db", Namespace: "ns", Port: 5432, Qualified: true}

	_, _, err := lookup(t.Context(), c, p, "default")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestResolveServiceMatchesLookup(t *testing.T) {
	svc := newService("ns", "web", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{
		{Name: "metrics", Port: 9090, TargetPort: intstrFromInt(9091)},
		{Name: "http", Port: 80, TargetPort: intstrFromInt(8080)},
	}
	// Two ready endpoints in two slices with different target ports, so the
	// per-slice port mapping is exercised and "the same endpoint as lookup" is
	// distinguishable from "any endpoint": a mutation that picks refs[len-1]
	// instead of refs[0] lands on web-2 and its slice's port.
	slice1 := newSlice("ns", "web",
		[]discoveryv1.EndpointPort{
			{Name: strPtr("metrics"), Port: int32Ptr(9091)},
			{Name: strPtr("http"), Port: int32Ptr(8080)},
		},
		endpoint("web-1", "10.1.0.5", nil),
	)
	slice2 := newSliceNamed("ns", "web", "web-slice-2",
		[]discoveryv1.EndpointPort{
			{Name: strPtr("metrics"), Port: int32Ptr(9092)},
			{Name: strPtr("http"), Port: int32Ptr(8081)},
		},
		endpoint("web-2", "10.1.0.6", nil),
	)
	c := newFakeClients(svc, slice1, slice2)
	// The explicit entry point must agree with the address path and must not
	// read pods: EndpointSlice readiness already carries that.
	c.cs.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		t.Errorf("ResolveService read a pod")
		return true, nil, errors.New("unexpected pod get")
	})

	pa, err := parseAddr("web.ns:80", "cluster.local")
	if err != nil {
		t.Fatalf("parseAddr: %v", err)
	}
	want, ok, err := lookup(t.Context(), c, pa, "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}

	got, err := ResolveService(t.Context(),
		c.cs.CoreV1().Services("ns"),
		c.cs.DiscoveryV1().EndpointSlices("ns"),
		"ns", "web", "http")
	if err != nil {
		t.Fatalf("ResolveService: %v", err)
	}
	if got != want {
		t.Fatalf("ResolveService = %+v, want %+v (same as lookup)", got, want)
	}
}

func newService(ns, name, clusterIP string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.ServiceSpec{ClusterIP: clusterIP},
	}
}

func newSlice(ns, svcName string, ports []discoveryv1.EndpointPort, eps ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return newSliceNamed(ns, svcName, svcName+"-slice", ports, eps...)
}

func newSliceNamed(ns, svcName, sliceName string, ports []discoveryv1.EndpointPort, eps ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sliceName,
			Labels:    map[string]string{"kubernetes.io/service-name": svcName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       ports,
		Endpoints:   eps,
	}
}

func endpoint(podName, ip string, ready *bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses:  []string{ip},
		Conditions: discoveryv1.EndpointConditions{Ready: ready},
		TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: podName},
	}
}

func hostnameEndpoint(hostname, ip, refName string) discoveryv1.Endpoint {
	ep := endpoint(refName, ip, nil)
	ep.Hostname = &hostname
	return ep
}

func pod(ns, name, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
	}
}

func readyPod(ns, name, ip string) *corev1.Pod {
	p := pod(ns, name, ip)
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return p
}

func intstrFromInt(i int32) intstr.IntOrString { return intstr.FromInt32(i) }

func boolPtr(b bool) *bool    { return &b }
func int32Ptr(i int32) *int32 { return &i }
func strPtr(s string) *string { return &s }
