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

func (a *PDBAnalyser) IsPodTakingAllBudget(ctx context.Context, pod *corev1.Pod) (bool, error) {
	pdbs, err := a.pdbIndexer.GetPDBsBlockedByPod(ctx, pod.GetName(), pod.GetNamespace())
	if err != nil {
		return false, err
	}

	// If there is no (blocked) PDB, it doesn't take any budget.
	// If there are multiple PDBs the pod cannot be evicted anyways.
	if len(pdbs) != 1 {
		return false, nil
	}
	pdb := pdbs[0]

	if isPDBLocked(pdb) {
		return false, nil
	}

	// If DesiredHealthy - CurrentHealthy is 1, we know that the given pod is taking the budget as it's not ready.
	// If it's bigger than 1, more pods included in the same PDB are taking the budget
	return (pdb.Status.DesiredHealthy - (pdb.Status.CurrentHealthy + 1)) == 0, nil
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
