package analyser

import (
	"context"
	"errors"

	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var _ Interface = &PDBAnalyser{}

// PDBAnalyser is an implementation of the analyser interface
type PDBAnalyser struct {
	podIndexer index.PodIndexer
	pdbIndexer index.PDBIndexer
}

// NewPDBAnalyser creates an instance of the PDB analyzer
func NewPDBAnalyser(indexer *index.Indexer) Interface {
	return &PDBAnalyser{podIndexer: indexer, pdbIndexer: indexer}
}

func (a *PDBAnalyser) BlockingPodsOnNode(ctx context.Context, nodeName string) ([]BlockingPod, error) {
	pods, err := a.podIndexer.GetPodsByNode(ctx, nodeName)
	if err != nil {
		return nil, err
	}

	blockingPods := make([]BlockingPod, 0)
	for _, pod := range pods {
		if utils.IsPodReady(pod) {
			// we are only interested in not ready pods
			continue
		}
		pdbs, err := a.pdbIndexer.GetPDBsBlockedByPod(ctx, pod.GetName(), pod.GetNamespace())
		if err != nil {
			return nil, errors.New("cannot get blocked pdbs by pod name")
		}

		for _, pdb := range pdbs {
			blockingPods = append(blockingPods, BlockingPod{
				NodeName: nodeName,
				Pod:      pod.DeepCopy(),
				PDB:      pdb.DeepCopy(),
			})
		}
	}

	return blockingPods, nil
}

func IsPDBBlocked(ctx context.Context, pod *corev1.Pod, pdb *policyv1.PodDisruptionBudget) bool {
	if isPDBLocked(pdb) {
		return true
	}

	// If the pod is not ready it's already taking budget from the PDB
	// If the remaining budget is still positive or zero, it's fine
	var podTakingBudget int32 = 0
	if utils.IsPodReady(pod) {
		podTakingBudget = 1
	}

	// CurrentHealthy - DesiredHealthy will give the currently available budget.
	// If the given pod is not ready, we know that it's taking some of the budget already, so we are increasing the number in that case.
	remainingBudget := ((pdb.Status.CurrentHealthy + podTakingBudget) - pdb.Status.DesiredHealthy)

	return remainingBudget <= 0
}

func isPDBLocked(pdb *policyv1.PodDisruptionBudget) bool {
	maxUnavailable := pdb.Spec.MaxUnavailable
	if maxUnavailable != nil {
		if maxUnavailable.Type == intstr.Int && maxUnavailable.IntVal == 0 {
			return true
		}
		if maxUnavailable.Type == intstr.String && maxUnavailable.StrVal == "0%" {
			return true
		}
	}

	minAvailable := pdb.Spec.MinAvailable
	if minAvailable != nil {
		// TODO think about the intval case
		// maybe something like minAvail > podCount
		if minAvailable.Type == intstr.String && minAvailable.StrVal == "100%" {
			return true
		}
	}

	return false
}
