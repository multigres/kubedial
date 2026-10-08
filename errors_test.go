package kubedial

import "testing"

func TestForwardErrorMessage(t *testing.T) {
	err := &ForwardError{Namespace: "ns", Pod: "pod", Port: 8080, Msg: "connection refused"}
	want := "kubedial: forward to ns/pod:8080: connection refused"
	if got := err.Error(); got != want {
		t.Errorf("ForwardError.Error() = %q, want %q", got, want)
	}
}
