package kubedial

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// parseAddr splits an address into the parts resolution needs. Rule 1 (the
// port) lives here; the host rules are shared with LookupHost, which parses a
// host with no port.
func parseAddr(addr, clusterDomain string) (parsedAddr, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return parsedAddr{}, fmt.Errorf("kubedial: address %q: %w", addr, err)
	}
	port, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return parsedAddr{}, fmt.Errorf("kubedial: port %q: %w", portStr, err)
	}
	if port < 1 || port > 65535 {
		return parsedAddr{}, fmt.Errorf("kubedial: port %d out of range 1-65535", port)
	}
	pa := parseHost(host, clusterDomain)
	pa.Port = int32(port)
	return pa, nil
}

// parseHost classifies a host with no port. A host that cannot be a
// Kubernetes name is kindExternal rather than an error: it is a legitimate
// address to dial directly, and only resolution decides reachability.
func parseHost(host, clusterDomain string) parsedAddr {
	host = strings.Trim(host, "[]")
	if ip := net.ParseIP(host); ip != nil {
		return parsedAddr{Kind: kindPodIP, Host: ip.String()}
	}

	host = strings.TrimSuffix(strings.ToLower(host), ".")

	// Strip a trailing dot from the configured domain too, so an FQDN form
	// like "cluster.local." matches as well as "cluster.local".
	domain := strings.TrimSuffix(strings.ToLower(clusterDomain), ".")
	qualified := false
	if suffix := ".svc." + domain; strings.HasSuffix(host, suffix) {
		host = strings.TrimSuffix(host, suffix)
		qualified = true
	} else if strings.HasSuffix(host, ".svc") {
		host = strings.TrimSuffix(host, ".svc")
		qualified = true
	}

	labels := strings.Split(host, ".")
	for _, label := range labels {
		if len(validation.IsDNS1123Label(label)) != 0 {
			return parsedAddr{Kind: kindExternal, Qualified: qualified}
		}
	}

	if qualified {
		switch len(labels) {
		case 2:
			return parsedAddr{Kind: kindService, Service: labels[0], Namespace: labels[1], Qualified: true}
		case 3:
			return parsedAddr{Kind: kindHeadlessPod, Host: labels[0], Service: labels[1], Namespace: labels[2], Qualified: true}
		default:
			return parsedAddr{Kind: kindExternal, Qualified: true}
		}
	}

	switch len(labels) {
	case 1:
		return parsedAddr{Kind: kindService, Service: labels[0]}
	case 2:
		return parsedAddr{Kind: kindService, Service: labels[0], Namespace: labels[1]}
	default:
		return parsedAddr{Kind: kindExternal}
	}
}
