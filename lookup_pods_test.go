package kubedial

import (
	"context"
	"errors"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var topoSelector = map[string]string{"app": "topo"}

// selectorService is a ClusterIP Service with a selector, which is what routes
// a lookup through pods instead of EndpointSlices.
func selectorService(ns, name string, ports ...corev1.ServicePort) *corev1.Service {
	svc := newService(ns, name, "10.0.0.1")
	svc.Spec.Selector = topoSelector
	svc.Spec.Ports = ports
	return svc
}

// selectedReadyPod is a labelled pod that the endpoint controller would publish.
func selectedReadyPod(ns, name, ip string, containerPorts ...corev1.ContainerPort) *corev1.Pod {
	p := pod(ns, name, ip)
	p.Labels = topoSelector
	p.Spec.Containers = []corev1.Container{{Name: "c", Ports: containerPorts}}
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return p
}

func lookupSvc(t *testing.T, c *fakeClients, port int32) (Endpoint, error) {
	t.Helper()
	p := parsedAddr{Kind: kindService, Service: "topo", Namespace: "ns", Port: port, Qualified: true}
	ep, ok, err := lookup(t.Context(), c, p, "default")
	if err == nil && !ok {
		t.Fatalf("lookup was not claimed as in-cluster")
	}
	return ep, err
}

// TestLookupSelectorServiceResolvesThroughPodsWithoutSlices is the point of the
// change: with EndpointSlices forbidden outright, a Service with a selector
// still resolves, from the Service and its pods alone.
func TestLookupSelectorServiceResolvesThroughPodsWithoutSlices(t *testing.T) {
	svc := selectorService("ns", "topo", corev1.ServicePort{Name: "client", Port: 2379, TargetPort: intstrFromInt(2379)})
	c := newFakeClients(svc, selectedReadyPod("ns", "topo-0", "10.1.0.4"))
	c.forbidSlices = true

	ep, err := lookupSvc(t, c, 2379)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	want := Endpoint{Namespace: "ns", Pod: "topo-0", PodIP: "10.1.0.4", Port: 2379}
	if ep != want {
		t.Fatalf("endpoint = %+v, want %+v", ep, want)
	}
}

func TestLookupSelectorServiceMapsNumericTargetPort(t *testing.T) {
	svc := selectorService("ns", "topo",
		corev1.ServicePort{Name: "metrics", Port: 9090, TargetPort: intstrFromInt(9091)},
		corev1.ServicePort{Name: "client", Port: 80, TargetPort: intstrFromInt(8080)},
	)
	c := newFakeClients(svc, selectedReadyPod("ns", "topo-0", "10.1.0.4"))
	c.forbidSlices = true

	ep, err := lookupSvc(t, c, 80)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ep.Port != 8080 {
		t.Fatalf("port = %d, want 8080 (the decoy listed first is 9091)", ep.Port)
	}
}

// TestLookupSelectorServiceUnsetTargetPortUsesServicePort: a zero targetPort
// means the Service port itself.
func TestLookupSelectorServiceUnsetTargetPortUsesServicePort(t *testing.T) {
	svc := selectorService("ns", "topo", corev1.ServicePort{Port: 2379})
	c := newFakeClients(svc, selectedReadyPod("ns", "topo-0", "10.1.0.4"))
	c.forbidSlices = true

	ep, err := lookupSvc(t, c, 2379)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ep.Port != 2379 {
		t.Fatalf("port = %d, want 2379", ep.Port)
	}
}

// TestLookupSelectorServiceResolvesNamedTargetPortPerPod: two pods behind one
// Service can declare the same port name on different numbers, and each is
// dialled on its own.
func TestLookupSelectorServiceResolvesNamedTargetPortPerPod(t *testing.T) {
	svc := selectorService("ns", "topo", corev1.ServicePort{Name: "client", Port: 2379, TargetPort: intstr.FromString("etcd")})
	c := newFakeClients(svc,
		selectedReadyPod("ns", "topo-0", "10.1.0.4", corev1.ContainerPort{Name: "etcd", ContainerPort: 2379}),
		selectedReadyPod("ns", "topo-1", "10.1.0.5", corev1.ContainerPort{Name: "etcd", ContainerPort: 12379}),
	)
	c.forbidSlices = true

	got := map[string]int32{}
	eps, ok, err := lookupAll(t.Context(), c, parsedAddr{Kind: kindService, Service: "topo", Namespace: "ns", Port: 2379, Qualified: true}, "default")
	if err != nil || !ok {
		t.Fatalf("lookupAll: ok=%v err=%v", ok, err)
	}
	for _, ep := range eps {
		got[ep.Pod] = ep.Port
	}
	if len(got) != 2 || got["topo-0"] != 2379 || got["topo-1"] != 12379 {
		t.Fatalf("ports by pod = %v, want topo-0:2379 topo-1:12379", got)
	}
}

func TestLookupSelectorServiceSkipsPodsTheEndpointControllerWouldNot(t *testing.T) {
	notReady := selectedReadyPod("ns", "topo-0", "10.1.0.4")
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse

	terminating := selectedReadyPod("ns", "topo-1", "10.1.0.5")
	now := metav1.Now()
	terminating.DeletionTimestamp = &now

	noIP := selectedReadyPod("ns", "topo-2", "")

	unlabelled := selectedReadyPod("ns", "other-0", "10.1.0.7")
	unlabelled.Labels = map[string]string{"app": "other"}

	good := selectedReadyPod("ns", "topo-3", "10.1.0.8")

	svc := selectorService("ns", "topo", corev1.ServicePort{Port: 2379, TargetPort: intstrFromInt(2379)})
	c := newFakeClients(svc, notReady, terminating, noIP, unlabelled, good)
	c.forbidSlices = true

	ep, err := lookupSvc(t, c, 2379)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ep.Pod != "topo-3" {
		t.Fatalf("pod = %q, want topo-3, the only one that is selected, ready, live and addressed", ep.Pod)
	}
}

func TestLookupSelectorServiceNoReadyPod(t *testing.T) {
	notReady := selectedReadyPod("ns", "topo-0", "10.1.0.4")
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse
	svc := selectorService("ns", "topo", corev1.ServicePort{Port: 2379, TargetPort: intstrFromInt(2379)})
	c := newFakeClients(svc, notReady)
	c.forbidSlices = true

	if _, err := lookupSvc(t, c, 2379); !errors.Is(err, ErrNoReadyEndpoint) {
		t.Fatalf("err = %v, want ErrNoReadyEndpoint", err)
	}
}

func TestLookupSelectorServiceUnknownServicePort(t *testing.T) {
	svc := selectorService("ns", "topo", corev1.ServicePort{Port: 2379, TargetPort: intstrFromInt(2379)})
	c := newFakeClients(svc, selectedReadyPod("ns", "topo-0", "10.1.0.4"))
	c.forbidSlices = true

	if _, err := lookupSvc(t, c, 9999); !errors.Is(err, ErrPortNotFound) {
		t.Fatalf("err = %v, want ErrPortNotFound", err)
	}
}

// TestLookupSelectorServiceNamedPortDeclaredNowhere: pods are ready but none
// declares the name, which is a port error and not a missing-endpoint one.
func TestLookupSelectorServiceNamedPortDeclaredNowhere(t *testing.T) {
	svc := selectorService("ns", "topo", corev1.ServicePort{Name: "client", Port: 2379, TargetPort: intstr.FromString("etcd")})
	c := newFakeClients(svc, selectedReadyPod("ns", "topo-0", "10.1.0.4", corev1.ContainerPort{Name: "other", ContainerPort: 1}))
	c.forbidSlices = true

	if _, err := lookupSvc(t, c, 2379); !errors.Is(err, ErrPortNotFound) {
		t.Fatalf("err = %v, want ErrPortNotFound", err)
	}
}

func TestLookupSelectorHeadlessServiceUsesPortAsIs(t *testing.T) {
	svc := selectorService("ns", "topo")
	svc.Spec.ClusterIP = corev1.ClusterIPNone
	c := newFakeClients(svc, selectedReadyPod("ns", "topo-0", "10.1.0.4"))
	c.forbidSlices = true

	ep, err := lookupSvc(t, c, 2379)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ep.Port != 2379 || ep.Pod != "topo-0" {
		t.Fatalf("endpoint = %+v, want topo-0 on the address port 2379", ep)
	}
}

// TestLookupSelectorServiceFallsBackToSlicesWhenPodsForbidden: a client that
// was only ever granted EndpointSlices keeps working. The slice carries a
// different pod from any pod in the namespace, so only the fallback can have
// produced the answer.
func TestLookupSelectorServiceFallsBackToSlicesWhenPodsForbidden(t *testing.T) {
	svc := selectorService("ns", "topo", corev1.ServicePort{Name: "client", Port: 2379, TargetPort: intstrFromInt(2379)})
	slice := newSlice("ns", "topo",
		[]discoveryv1.EndpointPort{{Name: strPtr("client"), Port: int32Ptr(2379)}},
		endpoint("topo-from-slice", "10.9.9.9", nil),
	)
	c := newFakeClients(svc, slice)
	c.forbid["ns"] = true

	ep, err := lookupSvc(t, c, 2379)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ep.Pod != "topo-from-slice" {
		t.Fatalf("pod = %q, want the slice's endpoint", ep.Pod)
	}
}

// TestLookupSelectorlessServiceStillUsesSlices: no selector, so the pods carry
// nothing to match and the slices are the only record of the endpoints. Pods
// are forbidden so that listing them would fail the lookup.
func TestLookupSelectorlessServiceStillUsesSlices(t *testing.T) {
	svc := newService("ns", "topo", "10.0.0.1")
	svc.Spec.Ports = []corev1.ServicePort{{Name: "client", Port: 2379, TargetPort: intstrFromInt(2379)}}
	slice := newSlice("ns", "topo",
		[]discoveryv1.EndpointPort{{Name: strPtr("client"), Port: int32Ptr(2379)}},
		endpoint("manual-0", "192.168.1.5", nil),
	)
	c := newFakeClients(svc, slice)
	c.forbid["ns"] = true

	ep, err := lookupSvc(t, c, 2379)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ep.Pod != "manual-0" {
		t.Fatalf("pod = %q, want manual-0", ep.Pod)
	}
}

func headlessPodAddr(host string) parsedAddr {
	return parsedAddr{Kind: kindHeadlessPod, Host: host, Service: "topo", Namespace: "ns", Port: 2379, Qualified: true}
}

func statefulPod(name, ip string) *corev1.Pod {
	p := pod("ns", name, ip)
	p.Spec.Subdomain = "topo"
	return p
}

// TestLookupHeadlessPodResolvesByNameWithoutSlices: a StatefulSet member
// address finds its pod directly, with EndpointSlices forbidden.
func TestLookupHeadlessPodResolvesByNameWithoutSlices(t *testing.T) {
	c := newFakeClients(statefulPod("topo-0", "10.1.0.4"), statefulPod("topo-1", "10.1.0.5"))
	c.forbidSlices = true

	ep, ok, err := lookup(t.Context(), c, headlessPodAddr("topo-1"), "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	want := Endpoint{Namespace: "ns", Pod: "topo-1", PodIP: "10.1.0.5", Port: 2379}
	if ep != want {
		t.Fatalf("endpoint = %+v, want %+v", ep, want)
	}
}

// TestLookupHeadlessPodRequiresTheServiceSubdomain: a pod that merely shares
// the name, in another Service's subdomain, is not the address's pod, so the
// lookup falls through to the slices and reports what they say.
func TestLookupHeadlessPodRequiresTheServiceSubdomain(t *testing.T) {
	stranger := statefulPod("topo-0", "10.1.0.4")
	stranger.Spec.Subdomain = "somewhere-else"
	c := newFakeClients(stranger)

	_, ok, err := lookup(t.Context(), c, headlessPodAddr("topo-0"), "default")
	if !errors.Is(err, ErrNotFound) || ok {
		t.Fatalf("ok=%v err=%v, want ErrNotFound from the empty slices", ok, err)
	}
}

func TestLookupHeadlessPodSkipsFinishedAndUnaddressedPods(t *testing.T) {
	done := statefulPod("topo-0", "10.1.0.4")
	done.Status.Phase = corev1.PodSucceeded
	noIP := statefulPod("topo-1", "")
	c := newFakeClients(done, noIP)

	for _, host := range []string{"topo-0", "topo-1"} {
		if _, _, err := lookup(t.Context(), c, headlessPodAddr(host), "default"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: err = %v, want ErrNotFound", host, err)
		}
	}
}

func TestLookupHeadlessPodFallsBackToSlicesWhenPodsForbidden(t *testing.T) {
	slice := newSlice("ns", "topo",
		[]discoveryv1.EndpointPort{{Name: strPtr("client"), Port: int32Ptr(2379)}},
		endpoint("topo-0", "10.9.9.9", nil),
	)
	c := newFakeClients(slice)
	c.forbid["ns"] = true

	ep, ok, err := lookup(t.Context(), c, headlessPodAddr("topo-0"), "default")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if ep.PodIP != "10.9.9.9" {
		t.Fatalf("PodIP = %q, want the slice's address", ep.PodIP)
	}
}

// reversedPods returns whatever the wrapped lister does, back to front, so a
// test can see whether a lookup orders its candidates itself or merely inherits
// the API server's order.
type reversedPods struct{ PodLister }

func (r reversedPods) List(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
	list, err := r.PodLister.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	slices.Reverse(list.Items)
	return list, nil
}

type reversedClients struct{ *fakeClients }

func (r reversedClients) Pods(ns string) PodLister { return reversedPods{r.fakeClients.Pods(ns)} }

// TestLookupSelectorServiceOrdersCandidatesByPodName: lookup takes the first
// candidate, and the spread picker indexes into the rest, so the order has to
// be a function of the pods and not of the list the API server returned.
func TestLookupSelectorServiceOrdersCandidatesByPodName(t *testing.T) {
	svc := selectorService("ns", "topo", corev1.ServicePort{Port: 2379, TargetPort: intstrFromInt(2379)})
	fc := newFakeClients(svc,
		selectedReadyPod("ns", "topo-0", "10.1.0.4"),
		selectedReadyPod("ns", "topo-1", "10.1.0.5"),
		selectedReadyPod("ns", "topo-2", "10.1.0.6"),
	)
	fc.forbidSlices = true

	eps, ok, err := lookupAll(t.Context(), reversedClients{fc}, parsedAddr{Kind: kindService, Service: "topo", Namespace: "ns", Port: 2379, Qualified: true}, "default")
	if err != nil || !ok {
		t.Fatalf("lookupAll: ok=%v err=%v", ok, err)
	}
	names := make([]string, 0, len(eps))
	for _, ep := range eps {
		names = append(names, ep.Pod)
	}
	if !slices.Equal(names, []string{"topo-0", "topo-1", "topo-2"}) {
		t.Fatalf("candidate order = %v, want by pod name", names)
	}
}
