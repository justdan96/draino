package drain_runner

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestDrainRunner(t *testing.T) {
	tests := []struct {
		Name          string
		Node          *corev1.Node
		Preprocessors []DrainPreprozessor
		Drainer       kubernetes.Drainer
	}{
		{
			Name:    "first test",
			Node:    createNode("isso", k8sclient.TaintDrainCandidate),
			Drainer: &kubernetes.NoopCordonDrainer{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			logger := logr.Discard()

			clientWrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{
				Objects: []runtime.Object{tt.Node},
				Indexes: []k8sclient.WithIndex{
					func(_ client.Client, cache cachecr.Cache) error {
						return groups.InitSchedulingGroupIndexer(cache, groups.NewGroupKeyFromNodeMetadata([]string{"key"}, nil, ""))
					},
				},
			})
			assert.NoError(t, err)

			fakeIndexer, err := index.New(clientWrapper.GetManagerClient(), clientWrapper.GetCache(), logger)
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			clientWrapper.Start(ch)

			retryWall, err := drain.NewRetryWall(clientWrapper.GetManagerClient(), logger, &drain.StaticRetryStrategy{Delay: time.Second, AlertThreashold: 5})
			assert.NoError(t, err)

			isso := &drainRunner{
				client:              clientWrapper.GetManagerClient(),
				logger:              logger,
				clock:               clock.RealClock{},
				retryWall:           retryWall,
				sharedIndexInformer: fakeIndexer,
				drainer:             tt.Drainer,
				runEvery:            time.Second,
				preprocessors:       tt.Preprocessors,
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			err = isso.Run(&groups.RunnerInfo{Context: ctx, Key: "isso"})
			assert.NoError(t, err)
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
