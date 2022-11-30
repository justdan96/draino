package k8sclient

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TaintNode(ctx context.Context, client client.Client, candidate *corev1.Node, now time.Time, values ...DrainTaintValue) error {
	var updated bool
	node := candidate

	for _, val := range values {
		taint := createTaint(val, now)
		newNode, changed, err := taints.AddOrUpdateTaint(node, taint)
		if err != nil {
			return err
		}
		if changed {
			updated = true
		}
		node = newNode
	}

	if !updated {
		return nil
	}

	return client.Update(ctx, node)
}

func UntaintNode(ctx context.Context, client client.Client, candidate *corev1.Node, values ...DrainTaintValue) error {
	var updated bool
	node := candidate

	for _, val := range values {
		taint := createTaint(val, time.Time{})
		newNode, changed, err := taints.AddOrUpdateTaint(node, taint)
		if err != nil {
			return err
		}
		if changed {
			updated = true
		}
		node = newNode
	}

	if !updated {
		return nil
	}

	return client.Update(ctx, node)
}

func createTaint(val DrainTaintValue, now time.Time) *corev1.Taint {
	timeAdded := metav1.NewTime(now)
	taint := corev1.Taint{Key: DrainoTaintKey, Value: val, Effect: corev1.TaintEffectNoSchedule, TimeAdded: &timeAdded}
	return &taint
}
