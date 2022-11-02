package analyser

import (
	"context"
	"errors"

	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
)

var _ Interface = &PDBAnalyser{}

type PDBAnalyser struct {
	podInformer index.PodIndexer
	pdbInformer index.PDBIndexer
}

func NewPDBAnalyser(indexer *index.Indexer) Interface {
	return &PDBAnalyser{podInformer: indexer, pdbInformer: indexer}
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
