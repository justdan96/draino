package drain_runner

import (
	"context"
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestDrainRunner(t *testing.T) {
	tests := []struct {
		Name          string
		Key           groups.GroupKey
		Node          *corev1.Node
		Preprocessors []DrainPreprozessor
		Drainer       kubernetes.Drainer
	}{
		{
			Name:    "first test",
			Key:     "my-key",
			Node:    createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer: &kubernetes.NoopCordonDrainer{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {

			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{
				Objects: []runtime.Object{tt.Node},
				Indexes: []k8sclient.WithIndex{
					func(_ client.Client, cache cachecr.Cache) error {
						return groups.InitSchedulingGroupIndexer(cache, groups.NewGroupKeyFromNodeMetadata([]string{"key"}, nil, ""))
					},
				},
			})
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			runner, err := NewFakeRunner(&FakeOptions{
				Chan:          ch,
				ClientWrapper: wrapper,
				Preprocessors: tt.Preprocessors,
				Drainer:       tt.Drainer,
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			err = runner.Run(&groups.RunnerInfo{Context: ctx, Key: tt.Key})
			assert.NoError(t, err)
			assert.NoError(t, ctx.Err(), "context reached deadline")

			var node corev1.Node
			err = wrapper.GetManagerClient().Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err)

			taint, exist := k8sclient.GetTaint(&node)
			assert.True(t, exist)
			assert.Equal(t, taint.Value, k8sclient.TaintDrained)
		})
	}
}

func createNode(key string, taintVal k8sclient.DrainTaintValue) *corev1.Node {
	taints := []corev1.Taint{}
	if taintVal != "" {
		taints = append(taints, *k8sclient.CreateTaint(taintVal, time.Now()))
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "foo-node",
			Labels: map[string]string{"key": key},
		},
		Spec: corev1.NodeSpec{
			Taints: taints,
		},
	}
}
