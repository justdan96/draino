package sync

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/clock"
)

func TestSyncNodeTaintToPods(t *testing.T) {
	tests := []struct {
		Name          string
		Node          *corev1.Node
		Pods          []runtime.Object
		PodFilterFunc kubernetes.PodFilterFunc

		ExpectedPodWithConditionCount int
	}{
		{
			Name: "Should set pod conditions",
			Node: createTestNode(t, "foo-node", true),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", false),
				createPodWithCondition("test-pod2", "test", "foo-node", false),
			},
			PodFilterFunc:                 filterAlwaysReturn,
			ExpectedPodWithConditionCount: 2,
		},
		{
			Name: "Should set pod conditions, no matter which pod namespace",
			Node: createTestNode(t, "foo-node", true),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", false),
				createPodWithCondition("test-pod2", "other-ns", "foo-node", false),
			},
			PodFilterFunc:                 filterAlwaysReturn,
			ExpectedPodWithConditionCount: 2,
		},
		{
			Name: "Should not touch pods on other nodes",
			Node: createTestNode(t, "foo-node", true),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", false),
				createPodWithCondition("test-pod2", "test", "second-node", false),
			},
			PodFilterFunc:                 filterAlwaysReturn,
			ExpectedPodWithConditionCount: 1,
		},
		{
			Name: "Should ignore pods that are not returned by the discovery func",
			Node: createTestNode(t, "foo-node", true),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", false),
				createPodWithCondition("test-pod2", "test", "foo-node", false),
			},
			PodFilterFunc:                 discoveryFuncThatIgnoresPodsWithName("test-pod"),
			ExpectedPodWithConditionCount: 1,
		},
		{
			Name:                          "Should not fail if there are no pods",
			Node:                          createTestNode(t, "foo-node", true),
			Pods:                          []runtime.Object{},
			PodFilterFunc:                 discoveryFuncThatIgnoresPodsWithName("test-pod"),
			ExpectedPodWithConditionCount: 0,
		},
		{
			Name: "Should remove conditions if taint got removed",
			Node: createTestNode(t, "foo-node", false),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", true),
				createPodWithCondition("test-pod2", "test", "foo-node", true),
			},
			PodFilterFunc:                 filterAlwaysReturn,
			ExpectedPodWithConditionCount: 0,
		},
		{
			Name: "Should remove condition from pods of other namespace",
			Node: createTestNode(t, "foo-node", false),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", true),
				createPodWithCondition("test-pod2", "other-ns", "foo-node", true),
			},
			PodFilterFunc:                 filterAlwaysReturn,
			ExpectedPodWithConditionCount: 0,
		},
		{
			Name: "Should not fail if there is no condition to be removed",
			Node: createTestNode(t, "foo-node", false),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", false),
				createPodWithCondition("test-pod2", "test", "foo-node", true),
			},
			PodFilterFunc:                 filterAlwaysReturn,
			ExpectedPodWithConditionCount: 0,
		},
		{
			Name: "Should only remove the condition from the actual node",
			Node: createTestNode(t, "foo-node", false),
			Pods: []runtime.Object{
				createPodWithCondition("test-pod", "test", "foo-node", true),
				createPodWithCondition("test-pod2", "test", "second-node", true),
			},
			PodFilterFunc:                 filterAlwaysReturn,
			ExpectedPodWithConditionCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: append(tt.Pods, tt.Node)})
			assert.NoError(t, err)

			ctx, cancelFn := context.WithCancel(context.Background())
			defer cancelFn()
			fakeIndexer, err := index.New(ctx, wrapper.GetManagerClient(), wrapper.GetCache(), logr.Discard())
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			synchronizer := NewNLATaintSynchronizer(wrapper.GetManagerClient(), logr.Discard(), clock.RealClock{}, fakeIndexer, tt.PodFilterFunc)
			err = synchronizer.SyncNodeTaintToPods(context.Background(), tt.Node)
			assert.NoError(t, err, "failed to sync node taint to pods")

			var podList corev1.PodList
			err = wrapper.GetManagerClient().List(context.Background(), &podList)
			assert.NoError(t, err, "failed to get pods for node")

			var podWithConditionConter int
			for _, pod := range podList.Items {
				if _, exists := k8sclient.GetPodNLACondition(&pod); exists {
					podWithConditionConter += 1
				}
			}

			assert.Equal(t, tt.ExpectedPodWithConditionCount, podWithConditionConter)
		})
	}
}

func discoveryFuncThatIgnoresPodsWithName(name string) kubernetes.PodFilterFunc {
	return func(p corev1.Pod) (pass bool, reason string, err error) {
		if p.Name == name {
			return false, "name_does_not_match", nil
		}
		return true, "", nil
	}
}

func filterAlwaysReturn(p corev1.Pod) (pass bool, reason string, err error) {
	return true, "", nil
}

func createTestNode(t *testing.T, name string, withNLATaint bool) *corev1.Node {
	var err error
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	if withNLATaint {
		node, _, err = taints.AddOrUpdateTaint(node, k8sclient.CreateNLATaint(k8sclient.TaintDrainCandidate, time.Now()))
		assert.NoError(t, err, "failed to add NLA taint to test node")

	}

	return node
}

func createPodWithCondition(name, ns, nodeName string, hasNLACondition bool) *corev1.Pod {
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
			Type:               k8sclient.PodNLAConditionType,
			Status:             corev1.ConditionTrue,
			Reason:             k8sclient.PodNLAConditionReason,
			Message:            k8sclient.PodNLAConditionMessage,
			LastProbeTime:      metav1.Now(),
			LastTransitionTime: metav1.Now(),
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Conditions: conditions,
		},
	}
}
