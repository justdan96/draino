package analyser

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

type BlockingPod struct {
	Node *corev1.Node
	Pod  *corev1.Pod
	PDB  *policyv1.PodDisruptionBudget
}

type Interface interface {
	BlockingPodsOnNode(ctx context.Context, node *corev1.Node) ([]BlockingPod, error)
}
