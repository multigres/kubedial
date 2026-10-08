package kubedial

import "testing"

func TestEndpointString(t *testing.T) {
	ep := Endpoint{Namespace: "prod", Pod: "db-0", PodIP: "10.0.0.5", Port: 5432}
	if got, want := ep.String(), "prod/db-0:5432"; got != want {
		t.Errorf("Endpoint.String() = %q, want %q", got, want)
	}
}

func TestModeString(t *testing.T) {
	tests := []struct {
		mode Mode
		want string
	}{
		{ModeAuto, "auto"},
		{ModeDirect, "direct"},
		{ModeForward, "forward"},
	}
	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.want {
			t.Errorf("Mode(%d).String() = %q, want %q", tt.mode, got, tt.want)
		}
	}
}
