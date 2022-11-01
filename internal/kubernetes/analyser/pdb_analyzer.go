package analyser

import (
	"context"
	"errors"

	"github.com/planetlabs/draino/internal/kubernetes/informer"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
)

var _ Interface = &PDBAnalyser{}

type PDBAnalyser struct {
	podInformer informer.PodInformer
	pdbInformer informer.PDBInformer
}

func NewPDBAnalyser(informer *informer.Informer) Interface {
	return &PDBAnalyser{podInformer: informer, pdbInformer: informer}
}

func (a *PDBAnalyser) BlockingPodsOnNode(ctx context.Context, node *corev1.Node) ([]BlockingPod, error) {
	pods, err := a.podInformer.GetPodsByNode(ctx, node.GetName())
	if err != nil {
		return nil, err
	}

	blockingPods := make([]BlockingPod, 0)
	for _, pod := range pods {
		if utils.IsPodReady(pod) {
			// we are only interested in not ready pods
			continue
		}
		pdbs, err := a.pdbInformer.GetPDBsBlockedByPod(ctx, pod.GetName(), pod.GetNamespace())
		if err != nil {
			return nil, errors.New("cannot get blocked pdbs by pod name")
		}

		for _, pdb := range pdbs {
			blockingPods = append(blockingPods, BlockingPod{
				Node: nil,
				Pod:  pod.DeepCopy(),
				PDB:  pdb.DeepCopy(),
			})
		}
	}

	return blockingPods, nil
}
