package analyser

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

type BlockingPod struct {
	NodeName string
	Pod      *corev1.Pod
	PDB      *policyv1.PodDisruptionBudget
}

type Interface interface {
	BlockingPodsOnNode(ctx context.Context, nodeName string) ([]BlockingPod, error)
}
