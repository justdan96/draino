package drain

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRetryWall(t *testing.T) {
	tests := []struct {
		Name          string
		Node          *corev1.Node
		Strategy      RetryStrategy
		ExpectedDelay *time.Duration
		Timestamp     time.Time
		Failures      int
	}{
		{
			Name: "Should properly set retry count annotation on node",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: "test-condition", Status: corev1.ConditionTrue}}},
			},
			Strategy:  &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			Timestamp: time.Now(),
			Failures:  2,
		},
		{
			Name: "Should properly calculate retry delay for node",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{{Type: "test-condition", Status: corev1.ConditionTrue}},
				},
			},
			Strategy:  &ExponentialRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			Timestamp: time.Now(),
			Failures:  3,
		},
		{
			Name: "Should return timestamp in the past if no retry was set",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{{Type: RetryWallConditionType, Status: corev1.ConditionTrue, Message: "0|test-message"}},
				},
			},
			Strategy:      &ExponentialRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ExpectedDelay: utils.DurationPtr(time.Duration(0)),
			Timestamp:     time.Now(),
			Failures:      0,
		},
		{
			Name: "Should retrun timestamp in the past if the condition message is poorly configured",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{{Type: RetryWallConditionType, Status: corev1.ConditionTrue, Message: "1"}},
				},
			},
			Strategy:      &ExponentialRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ExpectedDelay: utils.DurationPtr(time.Duration(0)),
			Timestamp:     time.Now(),
			Failures:      0,
		},
		{
			Name: "Should continue with drain attempts even if alert threshold was reached",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{{Type: "test-condition", Status: corev1.ConditionTrue}},
				},
			},
			Strategy:  &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 4},
			Timestamp: time.Now(),
			Failures:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			// setup everything
			client := fake.NewFakeClient(tt.Node)
			wall, err := NewRetryWall(client, logr.Discard(), tt.Strategy)
			assert.NoError(t, err, "cannot create retry wall")

			// make sure that the node will have no delay in the beginning
			retries := wall.GetDrainRetryAttemptsCount(tt.Node)
			assert.Equal(t, 0, retries, "should not return any retries in the beginning")
			retryTS := wall.GetRetryWallTimestamp(tt.Node)
			assert.True(t, time.Now().After(retryTS), "retry TS should be in the past")

			// inject drain failures
			for i := 0; i < tt.Failures; i++ {
				if err := wall.SetNewRetryWallTimestamp(context.Background(), tt.Node, "test-message", tt.Timestamp); err != nil {
					assert.NoError(t, err, "cannot mark node drain failure")
				}
			}

			// get latest version of node
			var node corev1.Node
			err = client.Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err, "cannot get node after drain failures")

			// check the expected delay
			retries = wall.GetDrainRetryAttemptsCount(&node)
			assert.Equal(t, tt.Failures, retries, "retry count from node should be equal to amount of injected errors")

			expectedDelay := tt.Strategy.GetDelay(tt.Failures)
			if tt.ExpectedDelay != nil {
				expectedDelay = *tt.ExpectedDelay
			}

			retryTS = wall.GetRetryWallTimestamp(&node)
			assert.NoError(t, err, "cannot get retry TS from node")
			// The idea is that the given timestamp + the expected delay + 1 second is always greater than the actual delay
			expectedTS := tt.Timestamp.Add(expectedDelay).Add(time.Second)
			assert.True(t, expectedTS.After(retryTS), "")

			// check if the condition was appended
			pos, condition, found := utils.FindNodeCondition(RetryWallConditionType, &node)
			assert.True(t, pos >= 0, "position should be bigger than or euqals to 0")
			assert.True(t, found, "node condition should be found")
			assert.NotEmpty(t, condition.Message, "condition message should have the proper failure count")

			// reset retry counter
			err = wall.ResetRetryCount(context.Background(), &node)
			assert.NoError(t, err, "cannot reset retry count on node")

			// get latest version of node
			node = corev1.Node{}
			err = client.Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err, "cannot get node after retry reset")

			// make sure that the condition got removed
			_, _, found = utils.FindNodeCondition(RetryWallConditionType, &node)
			assert.False(t, found, "should not find node drain retry condition")
		})
	}
}
