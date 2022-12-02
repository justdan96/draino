package drain_runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type failDrainer struct {
	kubernetes.NoopCordonDrainer
}

func (d *failDrainer) Drain(ctx context.Context, n *v1.Node) error { return errors.New("myerr") }

type testDrainPreprocessor interface {
	DrainPreprozessor
	Assert(*testing.T)
}

type testPreprocessor struct {
	madeCalls int
	// maxCalls is the amount of calls after Process will return true
	maxCalls int
	// expectedCalls is the amount we are expecting during the test
	// expectedCalls might be bigger than maxCalls if there is a second preprocessor which takes longer
	expectedCalls int
}

func (_ *testPreprocessor) GetName() string {
	return "testPreprocessor"
}

func (p *testPreprocessor) Process(node *corev1.Node) (bool, error) {
	p.madeCalls += 1
	return p.madeCalls >= p.maxCalls, nil
}

func (p *testPreprocessor) Assert(t *testing.T) {
	assert.Equal(t, p.expectedCalls, p.madeCalls)
}

func TestDrainRunner(t *testing.T) {
	tests := []struct {
		Name          string
		Key           groups.GroupKey
		Node          *corev1.Node
		Preprocessors []testDrainPreprocessor
		Drainer       kubernetes.Drainer

		ShoulHaveTaint  bool
		ExpectedTaint   k8sclient.DrainTaintValue
		ExpectedRetries int
	}{
		{
			Name:            "Should drain the node",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &kubernetes.NoopCordonDrainer{},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrained,
			ExpectedRetries: 0,
		},
		{
			Name:            "Should fail during drain and remove the candidate status from the node",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &failDrainer{},
			ShoulHaveTaint:  false,
			ExpectedRetries: 1,
		},
		{
			Name:            "Should ignore node without taint",
			Key:             "my-key",
			Node:            createNode("foo", ""),
			Drainer:         &failDrainer{},
			ShoulHaveTaint:  false,
			ExpectedRetries: 0,
		},
		{
			Name:            "Should not act on node with different key",
			Key:             "my-key",
			Node:            createNode("foo", k8sclient.TaintDrainCandidate),
			Drainer:         &failDrainer{},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrainCandidate,
			ExpectedRetries: 0,
		},
		{
			Name:            "Should wait for preprocessor to finish",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &kubernetes.NoopCordonDrainer{},
			Preprocessors:   []testDrainPreprocessor{&testPreprocessor{expectedCalls: 2, maxCalls: 2}},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrained,
			ExpectedRetries: 0,
		},
		{
			Name:            "Should wait for multiple preprocessors to finish even if one is taking longer than the other",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &kubernetes.NoopCordonDrainer{},
			Preprocessors:   []testDrainPreprocessor{&testPreprocessor{expectedCalls: 4, maxCalls: 2}, &testPreprocessor{expectedCalls: 4, maxCalls: 4}},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrained,
			ExpectedRetries: 0,
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
				Preprocessors: convertPreProcessors(tt.Preprocessors),
				Drainer:       tt.Drainer,
			})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err = runner.Run(&groups.RunnerInfo{Context: ctx, Key: tt.Key})
			assert.NoError(t, err)
			assert.NoError(t, ctx.Err(), "context reached deadline")

			var node corev1.Node
			err = wrapper.GetManagerClient().Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err)

			taint, exist := k8sclient.GetTaint(&node)
			if tt.ShoulHaveTaint {
				assert.True(t, exist)
				assert.Equal(t, tt.ExpectedTaint, taint.Value)
			} else {
				assert.False(t, exist)
			}

			drainAttempts := runner.retryWall.GetDrainRetryAttemptsCount(&node)
			assert.Equal(t, tt.ExpectedRetries, drainAttempts)

			for _, pre := range tt.Preprocessors {
				pre.Assert(t)
			}
		})
	}
}

func convertPreProcessors(tests []testDrainPreprocessor) []DrainPreprozessor {
	res := make([]DrainPreprozessor, 0, len(tests))
	for _, t := range tests {
		res = append(res, t)
	}
	return res
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
