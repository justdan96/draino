package informer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/runtime"
)

func Test_PodIndexer(t *testing.T) {
	tests := []struct {
		Name             string
		TestNodeName     string
		ExpectedPodNames []string
		Objects          []runtime.Object
	}{
		{
			Name:             "should find one pod on node",
			TestNodeName:     "my-node",
			ExpectedPodNames: []string{"my-test-pod"},
			Objects: []runtime.Object{
				createPod("my-test-pod", "default", "my-node", true),
				createPod("my-foo-pod-2", "default", "my-foo-node", true),
				createPod("my-foo-pod", "default", "my-foo-node", true),
			},
		},
		{
			Name:             "should find all pods for one node",
			TestNodeName:     "my-node",
			ExpectedPodNames: []string{"my-test-pod", "my-test-pod-2"},
			Objects: []runtime.Object{
				createPod("my-test-pod", "default", "my-node", true),
				createPod("my-test-pod-2", "default", "my-node", true),
				createPod("my-foo-pod", "default", "my-foo-node", true),
			},
		},
		{
			Name:             "should return empty array if nothing was found",
			TestNodeName:     "empty-node",
			ExpectedPodNames: []string{},
			Objects: []runtime.Object{
				createPod("my-test-pod", "default", "my-node", true),
				createPod("my-test-pod-2", "default", "my-node", true),
				createPod("my-foo-pod", "default", "my-foo-node", true),
			},
		},
		{
			Name:             "should not fail if no pods in cluster",
			TestNodeName:     "empty-node",
			ExpectedPodNames: []string{},
			Objects:          []runtime.Object{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ch := make(chan struct{})
			defer close(ch)

			informer, err := NewFakePodIndexer(ch, tt.Objects)
			assert.NoError(t, err)

			pods, err := informer.GetPodsByNode(context.TODO(), tt.TestNodeName)
			assert.NoError(t, err)

			assert.Equal(t, len(tt.ExpectedPodNames), len(pods), "received amount of pods to not match expected amount")
			for _, pod := range pods {
				assert.True(t, slices.Contains(tt.ExpectedPodNames, pod.GetName()), "found pod is not expected", pod.GetName())
			}
		})
	}
}
