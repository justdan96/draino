package drain

import (
	"context"
	"fmt"
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
		Failures      int
	}{
		{
			Name: "Should properly set retry count annotation on node",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: "test-condition", Status: corev1.ConditionTrue}}},
			},
			Strategy: &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			Failures: 2,
		},
		{
			Name: "Should properly calculate retry delay for node",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{{Type: "test-condition", Status: corev1.ConditionTrue}},
				},
			},
			Strategy: &ExponentialRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			Failures: 3,
		},
		{
			Name: "Should not return any delay if there was no failure",
			Node: &corev1.Node{
				ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{{Type: RetryWallConditionType, Status: corev1.ConditionTrue, Message: "0"}},
				},
			},
			Strategy:      &ExponentialRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ExpectedDelay: utils.DurationPtr(time.Duration(0)),
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
			Strategy: &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 4},
			Failures: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			// setup everything
			client := fake.NewFakeClient(tt.Node)
			wall, err := NewRetryWall(client, logr.Discard(), tt.Strategy)
			assert.NoError(t, err, "cannot create retry wall")

			// make sure that the node will have no delay in the beginning
			delay := wall.GetDelay(tt.Node)
			assert.Equal(t, time.Duration(0), delay)

			// inject drain failures
			for i := 0; i < tt.Failures; i++ {
				if err := wall.NoteDrainFailure(tt.Node); err != nil {
					assert.NoError(t, err, "cannot mark node drain failure")
				}
			}

			// get latest version of node
			var node corev1.Node
			err = client.Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err, "cannot get node after drain failures")

			// check the expected delay
			delay = wall.GetDelay(&node)
			expectedDelay := tt.Strategy.GetDelay(tt.Failures)
			if tt.ExpectedDelay != nil {
				expectedDelay = *tt.ExpectedDelay
			}
			assert.Equal(t, expectedDelay, delay)

			// check if the condition was appended
			pos, condition, found := utils.FindNodeCondition(RetryWallConditionType, &node)
			assert.True(t, pos >= 0, "position should be bigger than or euqals to 0")
			assert.True(t, found, "node condition should be found")
			assert.Equal(t, fmt.Sprintf("%d", tt.Failures), condition.Message, "condition message should have the proper failure count")

			// reset retry counter
			err = wall.ResetRetryCount(&node)
			assert.NoError(t, err)

			// get latest version of node
			node = corev1.Node{}
			err = client.Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err)

			// make sure that the condition got removed
			_, _, found = utils.FindNodeCondition(RetryWallConditionType, &node)
			assert.False(t, found)
		})
	}
}
