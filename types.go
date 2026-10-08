package kubedial

import "strconv"

// Endpoint is one pod port a stream can be opened to.
type Endpoint struct {
	Namespace string
	Pod       string
	PodIP     string
	Port      int32
}

func (e Endpoint) String() string {
	return e.Namespace + "/" + e.Pod + ":" + strconv.FormatInt(int64(e.Port), 10)
}

type addrKind int

const (
	kindExternal    addrKind = iota // not in-cluster: dial directly
	kindService                     // <svc>[.<ns>[.svc[.<domain>]]]
	kindHeadlessPod                 // <host>.<svc>.<ns>.svc[.<domain>]
	kindPodIP                       // IPv4/IPv6 literal
)

// parsedAddr is an address split into the parts resolution needs.
// Namespace is "" when the address did not name one.
type parsedAddr struct {
	Kind      addrKind
	Host      string // headless pod hostname, or the IP literal
	Service   string
	Namespace string
	Port      int32
	Qualified bool // true when the name ended in ".svc" or ".svc.<domain>"
}

// Mode selects how a Dialer reaches an address.
type Mode int

// Mode values.
const (
	ModeAuto Mode = iota
	ModeDirect
	ModeForward
)

func (m Mode) String() string {
	switch m {
	case ModeAuto:
		return "auto"
	case ModeDirect:
		return "direct"
	case ModeForward:
		return "forward"
	default:
		return "Mode(" + strconv.FormatInt(int64(m), 10) + ")"
	}
}
