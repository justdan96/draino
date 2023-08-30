package pkg

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type PreActivityState string

const (
	PreActivityStateWaiting    PreActivityState = ""
	PreActivityStateProcessing PreActivityState = "processing"
	PreActivityStateDone       PreActivityState = "done"
	PreActivityStateFailed     PreActivityState = "failed"
)

const (
	PreActivityAnnotation = "node-lifecycle.datadoghq.com/pre-activity-example"
	PodNLAConditionType   = "NodeLifecycle"
	PodNLAConditionReason = "NodeDrainCandidate"
)

// SetPodPreActivityAnnotationVal will set the given pre activity state to the pod.
func SetPodPreActivityAnnotationVal(ctx context.Context, kclient client.Client, pod *corev1.Pod, val PreActivityState) (*corev1.Pod, bool, error) {
	if pod.Annotations[PreActivityAnnotation] == string(val) {
		return pod, false, nil
	}

	// We have to do a deep copy to not mutate the given pointer
	newPod := pod.DeepCopy()
	newPod.Annotations[PreActivityAnnotation] = string(val)
	err := kclient.Patch(ctx, newPod, client.StrategicMergeFrom(pod))
	// If the pod is not found, it means that we can never update it
	if err != nil && errors.IsNotFound(err) {
		return pod, false, nil
	}

	return newPod, true, err
}

func HasPodNLACondition(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == PodNLAConditionType && condition.Reason == PodNLAConditionReason {
			return true
		}
	}
	return false
}
