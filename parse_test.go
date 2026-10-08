package kubedial

import (
	"net"
	"testing"
)

// parseCase is shared by TestParseAddr and TestParseHost so both classify the
// same hosts; the host test only differs in that it drops the port.
type parseCase struct {
	name          string
	addr          string
	clusterDomain string
	want          parsedAddr
	wantErr       bool
}

var parseCases = []parseCase{
	{
		name: "bare service",
		addr: "svc:80",
		want: parsedAddr{Kind: kindService, Service: "svc", Port: 80},
	},
	{
		name: "service and namespace",
		addr: "svc.ns:80",
		want: parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80},
	},
	{
		name: "qualified service",
		addr: "svc.ns.svc:80",
		want: parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80, Qualified: true},
	},
	{
		name: "fully qualified service",
		addr: "svc.ns.svc.cluster.local:80",
		want: parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80, Qualified: true},
	},
	{
		name:          "custom cluster domain",
		addr:          "svc.ns.svc.custom.domain:80",
		clusterDomain: "custom.domain",
		want:          parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80, Qualified: true},
	},
	{
		name:          "custom cluster domain with a trailing dot",
		addr:          "svc.ns.svc.cluster.local:80",
		clusterDomain: "cluster.local.",
		want:          parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80, Qualified: true},
	},
	{
		name:          "cluster domain is lowercased",
		addr:          "svc.ns.svc.cluster.local:80",
		clusterDomain: "CLUSTER.LOCAL",
		want:          parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80, Qualified: true},
	},
	{
		name: "custom domain suffix under the default domain is external",
		addr: "svc.ns.svc.custom.domain:80",
		want: parsedAddr{Kind: kindExternal, Port: 80},
	},
	{
		name: "headless pod, fully qualified",
		addr: "pod-0.svc-headless.ns.svc.cluster.local:5432",
		want: parsedAddr{Kind: kindHeadlessPod, Host: "pod-0", Service: "svc-headless", Namespace: "ns", Port: 5432, Qualified: true},
	},
	{
		name: "headless pod, qualified",
		addr: "pod-0.svc-headless.ns.svc:5432",
		want: parsedAddr{Kind: kindHeadlessPod, Host: "pod-0", Service: "svc-headless", Namespace: "ns", Port: 5432, Qualified: true},
	},
	{
		name: "IPv4 literal",
		addr: "10.1.2.3:8080",
		want: parsedAddr{Kind: kindPodIP, Host: "10.1.2.3", Port: 8080},
	},
	{
		name: "IPv6 literal",
		addr: "[fd00::1]:8080",
		want: parsedAddr{Kind: kindPodIP, Host: "fd00::1", Port: 8080},
	},
	{
		name: "two labels is service.namespace",
		addr: "example.com:443",
		want: parsedAddr{Kind: kindService, Service: "example", Namespace: "com", Port: 443},
	},
	{
		name: "three unqualified labels is external",
		addr: "api.example.com:443",
		want: parsedAddr{Kind: kindExternal, Port: 443},
	},
	{
		name: "host is lowercased",
		addr: "Svc.NS.svc:80",
		want: parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80, Qualified: true},
	},
	{
		name: "one trailing dot is stripped",
		addr: "svc.ns.svc.cluster.local.:80",
		want: parsedAddr{Kind: kindService, Service: "svc", Namespace: "ns", Port: 80, Qualified: true},
	},
	{
		name: "non-DNS label is external",
		addr: "under_score.ns.svc:80",
		want: parsedAddr{Kind: kindExternal, Port: 80, Qualified: true},
	},
	{
		name:    "port zero",
		addr:    "svc:0",
		wantErr: true,
	},
	{
		name:    "port too large",
		addr:    "svc:70000",
		wantErr: true,
	},
	{
		name:    "missing port",
		addr:    "svc",
		wantErr: true,
	},
}

func TestParseAddr(t *testing.T) {
	for _, tc := range parseCases {
		t.Run(tc.name, func(t *testing.T) {
			domain := tc.clusterDomain
			if domain == "" {
				domain = "cluster.local"
			}
			got, err := parseAddr(tc.addr, domain)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseAddr(%q) = %+v, want error", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAddr(%q) unexpected error: %v", tc.addr, err)
			}
			if got != tc.want {
				t.Errorf("parseAddr(%q) = %+v, want %+v", tc.addr, got, tc.want)
			}
		})
	}
}

func TestParseHost(t *testing.T) {
	for _, tc := range parseCases {
		if tc.wantErr {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			domain := tc.clusterDomain
			if domain == "" {
				domain = "cluster.local"
			}
			host, _, err := net.SplitHostPort(tc.addr)
			if err != nil {
				t.Fatalf("splitting %q: %v", tc.addr, err)
			}
			want := tc.want
			want.Port = 0
			if got := parseHost(host, domain); got != want {
				t.Errorf("parseHost(%q) = %+v, want %+v", host, got, want)
			}
		})
	}
}

func TestParseAddrLoopback(t *testing.T) {
	tests := []struct {
		addr string
		want parsedAddr
	}{
		{addr: "127.0.0.1:80", want: parsedAddr{Kind: kindPodIP, Host: "127.0.0.1", Port: 80}},
		{addr: "[::1]:80", want: parsedAddr{Kind: kindPodIP, Host: "::1", Port: 80}},
	}
	for _, tt := range tests {
		got, err := parseAddr(tt.addr, "cluster.local")
		if err != nil {
			t.Fatalf("parseAddr(%q) unexpected error: %v", tt.addr, err)
		}
		if got != tt.want {
			t.Errorf("parseAddr(%q) = %+v, want %+v", tt.addr, got, tt.want)
		}
	}
}
