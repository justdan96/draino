package analyser

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

type BlockingPDB struct {
	PodName       string
	PDB           *policyv1.PodDisruptionBudget
	BlockingPods  []*corev1.Pod
	BlockingNodes []*corev1.Node
}

type BlockingPod struct {
	Node *corev1.Node
	Pod  *corev1.Pod
	PDB  *policyv1.PodDisruptionBudget
}

type Interface interface {
	BlockingPodsOnNode(ctx context.Context, node *corev1.Node) ([]BlockingPod, error)
}
