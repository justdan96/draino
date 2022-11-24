package drain

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRetryWall(t *testing.T) {
	tests := []struct {
		Name            string
		Node            *corev1.Node
		Strategy        RetryStrategy
		DefaultStrategy string
		ExpectedDelay   *time.Duration
		FailureCount    int64
	}{
		{
			Name: "Should properly set retry count annotation on node",
			// in a unit test, jsonpatch needs a default annotations map, which is not empty
			Node:         &corev1.Node{ObjectMeta: v1.ObjectMeta{Name: "foo-node", Annotations: map[string]string{"foo": "bar"}}},
			Strategy:     &StaticRetryStrategy{Delay: time.Minute, MaxRetries: 10},
			FailureCount: 2,
		},
		{
			Name: "Should properly calculate retry delay for node",
			// in a unit test, jsonpatch needs a default annotations map, which is not empty
			Node:         &corev1.Node{ObjectMeta: v1.ObjectMeta{Name: "foo-node", Annotations: map[string]string{"foo": "bar"}}},
			Strategy:     &ExponentialRetryStrategy{Delay: time.Minute, MaxRetries: 10},
			FailureCount: 3,
		},
		{
			Name:          "Should not return any delay if there was no failure",
			Node:          &corev1.Node{ObjectMeta: v1.ObjectMeta{Name: "foo-node", Annotations: map[string]string{RetryWallCountAnnotation: "0"}}},
			Strategy:      &ExponentialRetryStrategy{Delay: time.Minute, MaxRetries: 10},
			ExpectedDelay: durationPtr(time.Duration(0)),
			FailureCount:  0,
		},
		{
			Name:          "Should deny new retry as max number is reached",
			Node:          &corev1.Node{ObjectMeta: v1.ObjectMeta{Name: "foo-node", Annotations: map[string]string{"foo": "bar"}}},
			Strategy:      &StaticRetryStrategy{MaxRetries: 4},
			ExpectedDelay: durationPtr(time.Duration(0)),
			FailureCount:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			// setup everything
			fakeClient := fake.NewFakeClient(tt.Node)
			retryWall := NewRetryWall(fakeClient, logr.Discard(), tt.Strategy)

			// make sure that the node will have no delay in the beginning
			duration, err := retryWall.GetDelay(tt.Node)
			assert.NoError(t, err)
			assert.Equal(t, time.Duration(0), duration, "There should be no delay in the beginning")

			// inject drain failures
			for i := 0; i < int(tt.FailureCount); i++ {
				retryWall.NoteDrainFailure(tt.Node)
			}

			// get latest version of node
			var node corev1.Node
			err = fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.Node.GetName()}, &node)
			assert.NoError(t, err)

			// check if patch was applied
			value, exist := node.GetAnnotations()[RetryWallCountAnnotation]
			assert.Equal(t, true, exist, "Retry annotation not set on node")
			intVal, err := strconv.ParseInt(value, 10, 64)
			assert.NoError(t, err)
			assert.Equal(t, tt.FailureCount, intVal, "Failure count on node should match expected count")

			// make sure that the result delay is as expected
			delay, err := retryWall.GetDelay(&node)
			assert.NoError(t, err)
			expectedDelay := tt.Strategy.GetDelay(int(tt.FailureCount))
			if tt.ExpectedDelay != nil {
				expectedDelay = *tt.ExpectedDelay
			}
			assert.Equal(t, expectedDelay, delay, "retry delay does not match expected result")

			// check if the counter reset works
			err = retryWall.ResetRetryCount(&node)
			assert.NoError(t, err)
			err = fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.Node.GetName()}, &node)
			assert.NoError(t, err)
			value, exist = node.GetAnnotations()[RetryWallCountAnnotation]
			assert.Equal(t, true, exist, "Retry annotation not set on node")
			intVal, err = strconv.ParseInt(value, 10, 64)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), intVal, "Failure count on node should match expected count")
		})
	}
}

func durationPtr(t time.Duration) *time.Duration {
	return &t
}
