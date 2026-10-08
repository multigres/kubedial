package kubedial

import (
	"testing"
	"time"
)

func TestOptionsDefaults(t *testing.T) {
	cfg := defaultConfig()
	if cfg.clusterDomain != "cluster.local" {
		t.Errorf("clusterDomain = %q, want %q", cfg.clusterDomain, "cluster.local")
	}
	if cfg.namespace != "" {
		t.Errorf("namespace = %q, want empty (the kubeconfig context's, else default)", cfg.namespace)
	}
	if cfg.mode != ModeAuto {
		t.Errorf("mode = %v, want %v", cfg.mode, ModeAuto)
	}
	if cfg.establishTimeout != 30*time.Second {
		t.Errorf("establishTimeout = %v, want %v", cfg.establishTimeout, 30*time.Second)
	}
	if cfg.positiveTTL != 10*time.Second {
		t.Errorf("positiveTTL = %v, want %v", cfg.positiveTTL, 10*time.Second)
	}
	if cfg.negativeTTL != 30*time.Second {
		t.Errorf("negativeTTL = %v, want %v", cfg.negativeTTL, 30*time.Second)
	}
	if cfg.podIdle != 2*time.Minute {
		t.Errorf("podIdle = %v, want %v", cfg.podIdle, 2*time.Minute)
	}
}
