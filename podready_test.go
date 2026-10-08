package kubedial

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodReady(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "ready condition true",
			pod:  readyPod("ns", "p", "10.1.0.1"),
			want: true,
		},
		{
			name: "ready condition false",
			pod: func() *corev1.Pod {
				p := pod("ns", "p", "10.1.0.1")
				p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
				return p
			}(),
			want: false,
		},
		{
			name: "no conditions",
			pod:  pod("ns", "p", "10.1.0.1"),
			want: false,
		},
		{
			name: "unrelated condition only",
			pod: func() *corev1.Pod {
				p := pod("ns", "p", "10.1.0.1")
				p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}}
				return p
			}(),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podReady(tt.pod); got != tt.want {
				t.Fatalf("podReady = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestActivePodsLess pins the ordering of podutils.ActivePods.Less that bestPod
// reverses, one rule at a time.
func TestActivePodsLess(t *testing.T) {
	base := func() *corev1.Pod {
		p := pod("ns", "p", "10.1.0.1")
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		return p
	}
	assigned := func() *corev1.Pod {
		p := base()
		p.Spec.NodeName = "n1"
		return p
	}
	t0 := metav1.NewTime(time.Unix(1000, 0))
	t1 := metav1.NewTime(time.Unix(2000, 0))

	tests := []struct {
		name string
		a, b *corev1.Pod
		want bool
	}{
		{
			name: "unassigned sorts before assigned",
			a:    base(),
			b:    assigned(),
			want: true,
		},
		{
			name: "assigned does not sort before unassigned",
			a:    assigned(),
			b:    base(),
			want: false,
		},
		{
			name: "pending sorts before running",
			a: func() *corev1.Pod {
				p := assigned()
				p.Status.Phase = corev1.PodPending
				return p
			}(),
			b:    assigned(),
			want: true,
		},
		{
			name: "not ready sorts before ready",
			a: func() *corev1.Pod {
				p := assigned()
				p.Status.Conditions = nil
				return p
			}(),
			b:    assigned(),
			want: true,
		},
		{
			name: "deleting sorts before not deleting",
			a: func() *corev1.Pod {
				p := assigned()
				p.DeletionTimestamp = &t0
				return p
			}(),
			b:    assigned(),
			want: true,
		},
		{
			name: "more recently ready sorts before longer ready",
			a: func() *corev1.Pod {
				p := assigned()
				p.Status.Conditions[0].LastTransitionTime = t1
				return p
			}(),
			b: func() *corev1.Pod {
				p := assigned()
				p.Status.Conditions[0].LastTransitionTime = t0
				return p
			}(),
			want: true,
		},
		{
			name: "more restarts sorts before fewer",
			a: func() *corev1.Pod {
				p := assigned()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{RestartCount: 3}}
				return p
			}(),
			b:    assigned(),
			want: true,
		},
		{
			name: "newer sorts before older",
			a: func() *corev1.Pod {
				p := assigned()
				p.CreationTimestamp = t1
				return p
			}(),
			b: func() *corev1.Pod {
				p := assigned()
				p.CreationTimestamp = t0
				return p
			}(),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := activePodsLess(tt.a, tt.b); got != tt.want {
				t.Fatalf("activePodsLess(a, b) = %v, want %v", got, tt.want)
			}
		})
	}
}
