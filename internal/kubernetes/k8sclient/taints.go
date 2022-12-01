package k8sclient

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/util/taints"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DrainTaintValue = string

const (
	DrainoTaintKey = "draino"

	TaintDrainCandidate DrainTaintValue = "drain-candidate"
	TaintDraining       DrainTaintValue = "draining"
	TaintDrained        DrainTaintValue = "drained"
)

func TaintNode(ctx context.Context, client client.Client, candidate *corev1.Node, now time.Time, value DrainTaintValue) error {
	var node corev1.Node
	err := client.Get(ctx, types.NamespacedName{Name: candidate.Name}, &node)
	if err != nil {
		return err
	}

	taint := CreateTaint(value, now)
	newNode, updated, err := taints.AddOrUpdateTaint(&node, taint)
	if err != nil {
		return err
	}
	if !updated {
		return nil
	}
	return client.Update(ctx, newNode)
}

func UntaintNode(ctx context.Context, client client.Client, candidate *corev1.Node) error {
	// The neither the taint value nor the timestamp do really matter
	taint := CreateTaint(TaintDrained, time.Time{})
	newNode, updated, err := taints.RemoveTaint(candidate, taint)
	if err != nil {
		return err
	}
	if !updated {
		return nil
	}
	return client.Update(ctx, newNode)
}

func GetTaint(node *corev1.Node) (*corev1.Taint, bool) {
	if len(node.Spec.Taints) == 0 {
		return nil, false
	}

	// The neither the taint value nor the timestamp do really matter
	search := CreateTaint(TaintDrainCandidate, time.Time{})
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(search) {
			return &taint, true
		}
	}

	return nil, false
}

func CreateTaint(val DrainTaintValue, now time.Time) *corev1.Taint {
	timeAdded := metav1.NewTime(now)
	taint := corev1.Taint{Key: DrainoTaintKey, Value: val, Effect: corev1.TaintEffectNoSchedule, TimeAdded: &timeAdded}
	return &taint
}
