package k8sclient

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	PodNLAConditionType    = "NodeLifecycle"
	PodNLAConditionReason  = "NodeDrainCandidate"
	PodNLAConditionMessage = "Node was selected as drain candidate."
)

// GetPodNLACondition will return a copy the NLA condition if found.
// If not, it will return nil and false.
// The function only cares about the condition type and not reason or message.
func GetPodNLACondition(pod *corev1.Pod) (*corev1.PodCondition, bool) {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == PodNLAConditionType {
			return condition.DeepCopy(), true
		}
	}

	return nil, false
}

// SetOrUpdatePodNLACondition will create the NLA condition on the given Pod. In case it exists already, it will update LastProbeTime to the given TS.
// As a result, it will return the new Pod, with the updated condition.
func SetOrUpdatePodNLACondition(ctx context.Context, kubeClient client.Client, pod *corev1.Pod, ts time.Time) (*corev1.Pod, error) {
	newPod := pod.DeepCopy()

	idx := -1
	for i, condition := range newPod.Status.Conditions {
		if condition.Type == PodNLAConditionType {
			idx = i
		}
	}

	if idx >= 0 {
		newPod.Status.Conditions[idx].LastProbeTime = metav1.NewTime(ts)
	} else {
		newPod.Status.Conditions = append(newPod.Status.Conditions, corev1.PodCondition{
			Type:               PodNLAConditionType,
			Status:             corev1.ConditionTrue,
			LastProbeTime:      metav1.NewTime(ts),
			LastTransitionTime: metav1.NewTime(ts),
			Reason:             PodNLAConditionReason,
			Message:            PodNLAConditionMessage,
		})
	}

	// The strategic merge path will only update the specific condition
	err := kubeClient.SubResource("status").Patch(ctx, newPod, client.StrategicMergeFrom(pod))
	if err != nil {
		return nil, err
	}

	return newPod, nil
}

// RemovePodNLACondition will remove the NLA condition from the given pod, if it has it.
func RemovePodNLACondition(ctx context.Context, kubeClient client.Client, pod *corev1.Pod) (*corev1.Pod, bool, error) {
	if _, exists := GetPodNLACondition(pod); !exists {
		return pod, false, nil
	}

	newPod := pod.DeepCopy()
	newConditions := make([]corev1.PodCondition, 0, (len(newPod.Status.Conditions)))
	for _, condition := range newPod.Status.Conditions {
		if condition.Type != PodNLAConditionType {
			newConditions = append(newConditions, condition)
		}
	}

	newPod.Status.Conditions = newConditions
	// The strategic merge path will only update the specific condition
	err := kubeClient.SubResource("status").Patch(ctx, newPod, client.StrategicMergeFrom(pod))
	if err != nil {
		return nil, false, err
	}

	return newPod, true, nil
}
