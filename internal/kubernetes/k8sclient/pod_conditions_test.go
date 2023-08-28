package k8sclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetPodNLACondition(t *testing.T) {
	tests := []struct {
		Name            string
		Pod             *corev1.Pod
		ExpectCondition bool
	}{
		{
			Name:            "Should find condition",
			Pod:             createPodWithCondition(true, time.Now()),
			ExpectCondition: true,
		},
		{
			Name:            "Should not find condition",
			Pod:             createPodWithCondition(false, time.Now()),
			ExpectCondition: false,
		},
		{
			Name: "Should not find condition with same reason",
			Pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "test",
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:          "OtherType",
							Status:        corev1.ConditionTrue,
							Reason:        PodNLAConditionReason,
							Message:       PodNLAConditionMessage,
							LastProbeTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			ExpectCondition: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, exists := GetPodNLACondition(tt.Pod)
			assert.Equal(t, tt.ExpectCondition, exists)
		})
	}
}

func TestSetOrUpdatePodNLACondition(t *testing.T) {
	tests := []struct {
		Name               string
		Pod                *corev1.Pod
		TS                 time.Time
		ExpectNewCondition bool
	}{
		{
			Name:               "Should set condition if not there yet",
			Pod:                createPodWithCondition(false, time.Now()),
			TS:                 time.Now(),
			ExpectNewCondition: true,
		},
		{
			Name:               "Should update condition probe time",
			Pod:                createPodWithCondition(true, time.Now().Add(-time.Minute)),
			TS:                 time.Now(),
			ExpectNewCondition: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithObjects(tt.Pod).Build()

			newPod, err := SetOrUpdatePodNLACondition(context.Background(), fakeClient, tt.Pod, tt.TS)
			assert.NoError(t, err, "failed to set pod condition")
			assert.NotEqual(t, tt.Pod, newPod, "should not be same pointer")

			var freshPod corev1.Pod
			err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: tt.Pod.Namespace, Name: tt.Pod.Name}, &freshPod)
			assert.NoError(t, err, "failed to get fresh pod")

			condition, exists := GetPodNLACondition(&freshPod)
			assert.True(t, exists, "should have condition set")
			// We have to format it, because the kubernetes object TS is losing some precision
			assert.Equal(t, tt.TS.Format(time.RFC3339), condition.LastProbeTime.Format(time.RFC3339), "should always update last probe time")

			if tt.ExpectNewCondition {
				// We have to format it, because the kubernetes object TS is losing some precision
				assert.Equal(t, tt.TS.Format(time.RFC3339), condition.LastTransitionTime.Time.Format(time.RFC3339))
				// Make sure that we add a new condition and don't overwrite another one
				assert.Equal(t, (len(tt.Pod.Status.Conditions) + 1), len(freshPod.Status.Conditions))
			} else {
				// We have to format it, because the kubernetes object TS is losing some precision
				assert.NotEqual(t, tt.TS.Format(time.RFC3339), condition.LastTransitionTime.Time.Format(time.RFC3339))
				// Make sure we don't create a new condition
				assert.Equal(t, len(tt.Pod.Status.Conditions), len(freshPod.Status.Conditions))
			}
		})
	}
}

func TestRemovePodNLACondition(t *testing.T) {
	tests := []struct {
		Name            string
		Pod             *corev1.Pod
		ExpectedRemoved bool
		ExpectedNewPod  bool
	}{
		{
			Name:            "Should remove condition from pod",
			Pod:             createPodWithCondition(true, time.Now()),
			ExpectedRemoved: true,
			ExpectedNewPod:  true,
		},
		{
			Name:            "Should not do anything if pod doesn't have condition",
			Pod:             createPodWithCondition(false, time.Now()),
			ExpectedRemoved: false,
			ExpectedNewPod:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithObjects(tt.Pod).Build()

			newPod, removed, err := RemovePodNLACondition(context.Background(), fakeClient, tt.Pod)
			assert.NoError(t, err, "failed to set pod condition")
			assert.Equal(t, tt.ExpectedRemoved, removed)

			if tt.ExpectedNewPod {
				assert.NotEqual(t, tt.Pod, newPod, "should not be same pointer")
			} else {
				assert.Equal(t, tt.Pod, newPod, "should not be same pointer")
			}

			var freshPod corev1.Pod
			err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: tt.Pod.Namespace, Name: tt.Pod.Name}, &freshPod)
			assert.NoError(t, err, "failed to get fresh pod")

			condition, exists := GetPodNLACondition(&freshPod)
			assert.Nil(t, condition, "should not find any condition")
			assert.False(t, exists, "should not have condition set")

			// Make sure that only our condition was removed
			if tt.ExpectedRemoved {
				assert.Equal(t, (len(tt.Pod.Status.Conditions) - 1), len(freshPod.Status.Conditions))
			} else {
				assert.Equal(t, len(tt.Pod.Status.Conditions), len(freshPod.Status.Conditions))
			}
		})
	}
}

func createPodWithCondition(hasNLACondition bool, time time.Time) *corev1.Pod {
	conditions := []corev1.PodCondition{
		{
			Type:               "RandomCondition",
			Status:             corev1.ConditionFalse,
			LastProbeTime:      metav1.Now(),
			LastTransitionTime: metav1.Now(),
		},
	}
	if hasNLACondition {
		conditions = append(conditions, corev1.PodCondition{
			Type:               PodNLAConditionType,
			Status:             corev1.ConditionTrue,
			Reason:             PodNLAConditionReason,
			Message:            PodNLAConditionMessage,
			LastProbeTime:      metav1.NewTime(time),
			LastTransitionTime: metav1.NewTime(time),
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test",
		},
		Status: corev1.PodStatus{
			Conditions: conditions,
		},
	}
}
