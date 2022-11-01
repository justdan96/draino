package informer

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const PDBBlockByPodIdx = "pod:blocking:pdb"

type PDBInformer interface {
	GetPDBsBlockedByPod(ctx context.Context, podName, ns string) ([]*policyv1.PodDisruptionBudget, error)
}

func (i *Informer) GetPDBsBlockedByPod(ctx context.Context, podName, ns string) ([]*policyv1.PodDisruptionBudget, error) {
	return GetFromIndex[*policyv1.PodDisruptionBudget](ctx, i, PDBBlockByPodIdx, ns, podName)
}

func initPDBIndexer(clt client.Client, cache cachecr.Cache) error {
	return cache.IndexField(
		context.Background(),
		&policyv1.PodDisruptionBudget{},
		PDBBlockByPodIdx,
		func(o client.Object) []string {
			return indexPDBBlockingPod(clt, o)
		},
	)
}

func indexPDBBlockingPod(clt client.Client, o client.Object) []string {
	pdb, ok := o.(*policyv1.PodDisruptionBudget)
	if !ok {
		return []string{}
	}

	blockingPods, _ := getBlockingPodsForPDB(clt, pdb)
	return blockingPods
}

func getBlockingPodsForPDB(clt client.Client, pdb *policyv1.PodDisruptionBudget) ([]string, error) {
	var pods corev1.PodList
	if err := clt.List(context.Background(), &pods, &client.ListOptions{Namespace: pdb.GetNamespace()}); err != nil {
		return []string{}, err
	}

	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return []string{}, err
	}

	blockingPods := make([]string, 0)
	for _, pod := range pods.Items {
		if !utils.IsPodReady(&pod) {
			continue
		}

		ls := labels.Set(pod.GetLabels())
		if !selector.Matches(ls) {
			continue
		}

		blockingPods = append(blockingPods, pod.GetName())
	}

	return blockingPods, nil
}
