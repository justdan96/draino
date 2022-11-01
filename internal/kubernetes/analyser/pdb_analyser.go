package analyser

import (
	"context"
	"errors"

	"github.com/planetlabs/draino/internal/kubernetes/informer"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
)

var _ Interface = &PDBAnalyser{}

type PDBAnalyser struct {
	podInformer informer.PodInformer
	pdbInformer informer.PDBInformer
}

func NewPDBAnalyser(informer *informer.Informer) Interface {
	return &PDBAnalyser{podInformer: informer, pdbInformer: informer}
}

func (a *PDBAnalyser) BlockingPodsOnNode(ctx context.Context, nodeName string) ([]BlockingPod, error) {
	pods, err := a.podInformer.GetPodsByNode(ctx, nodeName)
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
				NodeName: nodeName,
				Pod:      pod.DeepCopy(),
				PDB:      pdb.DeepCopy(),
			})
		}
	}

	return blockingPods, nil
}
