/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubedial

// podReady and activePodsLess are copied from
// k8s.io/kubectl/pkg/util/podutils/podutils.go at v0.37.0 (Apache-2.0), so
// kubedial can order pods the way kubectl does without depending on kubectl.

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// podReady reports whether a pod's Ready condition is true.
func podReady(pod *corev1.Pod) bool {
	return podReadyConditionTrue(pod.Status)
}

func podReadyConditionTrue(status corev1.PodStatus) bool {
	condition := podReadyCondition(status)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

func podReadyCondition(status corev1.PodStatus) *corev1.PodCondition {
	_, condition := podCondition(&status, corev1.PodReady)
	return condition
}

func podCondition(status *corev1.PodStatus, conditionType corev1.PodConditionType) (int, *corev1.PodCondition) {
	if status == nil {
		return -1, nil
	}
	return podConditionFromList(status.Conditions, conditionType)
}

func podConditionFromList(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) (int, *corev1.PodCondition) {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return i, &conditions[i]
		}
	}
	return -1, nil
}

func podDeleting(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

// activePodsLess is the ordering of podutils.ActivePods.Less: it sorts the pod
// a controller would delete first to the front.
func activePodsLess(a, b *corev1.Pod) bool {
	// 1. Unassigned < assigned.
	if a.Spec.NodeName != b.Spec.NodeName && (len(a.Spec.NodeName) == 0 || len(b.Spec.NodeName) == 0) {
		return len(a.Spec.NodeName) == 0
	}
	// 2. PodPending < PodUnknown < PodRunning.
	phase := map[corev1.PodPhase]int{corev1.PodPending: 0, corev1.PodUnknown: 1, corev1.PodRunning: 2}
	if phase[a.Status.Phase] != phase[b.Status.Phase] {
		return phase[a.Status.Phase] < phase[b.Status.Phase]
	}
	// 3. Not ready < ready.
	if podReady(a) != podReady(b) {
		return !podReady(a)
	}
	// 4. Deleting < not deleting.
	if podDeleting(a) != podDeleting(b) {
		return podDeleting(a)
	}
	// 5. Older deletion timestamp < newer deletion timestamp.
	if podDeleting(a) && podDeleting(b) && !a.DeletionTimestamp.Equal(b.DeletionTimestamp) {
		return a.DeletionTimestamp.Before(b.DeletionTimestamp)
	}
	// 6. Been ready for empty time < less time < more time.
	if podReady(a) && podReady(b) && !podReadyTime(a).Equal(podReadyTime(b)) {
		return afterOrZero(podReadyTime(a), podReadyTime(b))
	}
	// 7. Higher restart counts < lower restart counts.
	if res := compareMaxContainerRestarts(a, b); res != nil {
		return *res
	}
	// 8. Empty creation time pods < newer pods < older pods.
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return afterOrZero(&a.CreationTimestamp, &b.CreationTimestamp)
	}
	return false
}

// afterOrZero reports whether t1 is after t2, with the zero time ordered after
// every non-zero time.
func afterOrZero(t1, t2 *metav1.Time) bool {
	if t1.Time.IsZero() || t2.Time.IsZero() {
		return t1.Time.IsZero()
	}
	return t1.After(t2.Time)
}

func podReadyTime(pod *corev1.Pod) *metav1.Time {
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return &c.LastTransitionTime
		}
	}
	return &metav1.Time{}
}

func maxContainerRestarts(pod *corev1.Pod) (regularRestarts, sidecarRestarts int) {
	for i := range pod.Status.ContainerStatuses {
		regularRestarts = max(regularRestarts, int(pod.Status.ContainerStatuses[i].RestartCount))
	}
	sidecars := make(map[string]struct{}, len(pod.Spec.InitContainers))
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sidecars[c.Name] = struct{}{}
		}
	}
	for i := range pod.Status.InitContainerStatuses {
		c := &pod.Status.InitContainerStatuses[i]
		if _, ok := sidecars[c.Name]; ok {
			sidecarRestarts = max(sidecarRestarts, int(c.RestartCount))
		}
	}
	return regularRestarts, sidecarRestarts
}

// compareMaxContainerRestarts reports which pod has the higher restart count:
// true for a, false for b, nil when they are equal.
func compareMaxContainerRestarts(a, b *corev1.Pod) *bool {
	aRegular, aSidecar := maxContainerRestarts(a)
	bRegular, bSidecar := maxContainerRestarts(b)
	if aRegular != bRegular {
		res := aRegular > bRegular
		return &res
	}
	if aSidecar != bSidecar {
		res := aSidecar > bSidecar
		return &res
	}
	return nil
}
