package pre_processor

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
)

func TestNodeReplacementPreProcessor(t *testing.T) {
	trueVal := PreprovisioningAnnotationValue
	falseVal := PreprovisioningFalseAnnotationValue

	requestedVal := kubernetes.NodeLabelKeyReplaceRequest
	doneVal := kubernetes.NodeLabelValueReplaceDone
	failedVal := kubernetes.NodeLabelValueReplaceFailed
	unknownVal := "unknow-state"

	tests := []struct {
		Name                     string
		ReplaceAllNodesByDefault bool
		Node                     corev1.Node

		ExpectedResult bool
		ExpectedReason PreProcessNotDoneReason
		ExpectLabel    bool
	}{
		{
			Name:                     "Should ignore node as it doesn't have the annotation",
			ReplaceAllNodesByDefault: false,
			Node:                     createNodeToReplace(nil, nil),
			ExpectedResult:           true,
			ExpectLabel:              false,
		},
		{
			Name:                     "Should add replacement label to node as it's opted in by default",
			ReplaceAllNodesByDefault: true,
			Node:                     createNodeToReplace(nil, nil),
			ExpectedResult:           false,
			ExpectedReason:           PreProcessNotDoneReasonProcessing,
			ExpectLabel:              true,
		},
		{
			Name:                     "Should not replace node even if opt-in es active by default",
			ReplaceAllNodesByDefault: true,
			Node:                     createNodeToReplace(&falseVal, nil),
			ExpectedResult:           true,
			ExpectLabel:              false,
		},
		{
			Name:                     "Should add replacement label if opted in",
			ReplaceAllNodesByDefault: false,
			Node:                     createNodeToReplace(&trueVal, nil),
			ExpectedResult:           false,
			ExpectedReason:           PreProcessNotDoneReasonProcessing,
			ExpectLabel:              true,
		},
		{
			Name:                     "Should finish if replacement was successfull",
			ReplaceAllNodesByDefault: false,
			Node:                     createNodeToReplace(&trueVal, &doneVal),
			ExpectedResult:           true,
			ExpectLabel:              true,
		},
		{
			Name:                     "Should wait until replacement was done",
			ReplaceAllNodesByDefault: false,
			Node:                     createNodeToReplace(&trueVal, &requestedVal),
			ExpectedResult:           false,
			ExpectedReason:           PreProcessNotDoneReasonProcessing,
			ExpectLabel:              true,
		},
		{
			Name:                     "Should ignore replacement states that it doesn't know",
			ReplaceAllNodesByDefault: false,
			Node:                     createNodeToReplace(&trueVal, &unknownVal),
			ExpectedResult:           false,
			ExpectedReason:           PreProcessNotDoneReasonProcessing,
			ExpectLabel:              true,
		},
		{
			Name:                     "Should return error if replacement failed",
			ReplaceAllNodesByDefault: false,
			Node:                     createNodeToReplace(&trueVal, &failedVal),
			ExpectedResult:           false,
			ExpectedReason:           PreProcessNotDoneReasonFailure,
			ExpectLabel:              true,
		},
		{
			Name:                     "Should return timeout error if preprocessing takes too long",
			ReplaceAllNodesByDefault: false,
			Node:                     createNodeWithTimeToReplace(&trueVal, &failedVal, time.Now().Add(-2*time.Hour)),
			ExpectedResult:           false,
			ExpectedReason:           PreProcessNotDoneReasonTimeout,
			ExpectLabel:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			client := fake.NewFakeClient(&tt.Node)
			pre := NewNodeReplacementPreProcessor(client, tt.ReplaceAllNodesByDefault, logr.Discard(), &clock.RealClock{})

			res, reason, err := pre.IsDone(context.Background(), tt.Node.DeepCopy())
			assert.Equal(t, tt.ExpectedResult, res)
			assert.Equal(t, tt.ExpectedReason, reason)
			assert.NoError(t, err)

			exist, err := hasNodeReplacementLabel(client, tt.Node.Name)
			assert.NoError(t, err)
			assert.Equal(t, tt.ExpectLabel, exist)
		})
	}
}

func createNodeToReplace(annotationVal *string, labelVal *string) corev1.Node {
	return createNodeWithTimeToReplace(annotationVal, labelVal, time.Now())
}

func createNodeWithTimeToReplace(annotationVal *string, labelVal *string, time time.Time) corev1.Node {
	annotaions := map[string]string{}
	if annotationVal != nil {
		annotaions[PreprovisioningAnnotationKey] = *annotationVal
	}
	labels := map[string]string{}
	if labelVal != nil {
		labels[kubernetes.NodeLabelKeyReplaceRequest] = *labelVal
	}
	taintAdded := metav1.NewTime(time)
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-node",
			Annotations: annotaions,
			Labels:      labels,
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:       k8sclient.DrainoTaintKey,
					Value:     k8sclient.TaintDrainCandidate,
					Effect:    corev1.TaintEffectNoSchedule,
					TimeAdded: &taintAdded,
				},
			},
		},
	}
}

func hasNodeReplacementLabel(client client.Client, nodeName string) (bool, error) {
	var node corev1.Node
	err := client.Get(context.Background(), types.NamespacedName{Name: nodeName}, &node)
	if err != nil {
		return false, err
	}

	exist := false
	if node.Labels != nil {
		_, exist = node.Labels[kubernetes.NodeLabelKeyReplaceRequest]
	}
	return exist, nil
}
