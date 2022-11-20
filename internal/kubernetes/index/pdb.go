package index

import (
	"context"
	"errors"
	"fmt"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
)

// PDBBlockByPodIdx is the index key used to index blocking pods for each PDB
const PDBBlockByPodIdx = "pod:blocking:pdb"

// PDBIndexer abstracts all the methods related to PDB based indices
type PDBIndexer interface {
	// GetPDBsBlockedByPod will return a list of PDBs that are blocked by the given pod.
	// This means that the disruption budget are used by the Pod.
	GetPDBsBlockedByPod(ctx context.Context, podName, ns string) ([]*policyv1.PodDisruptionBudget, error)

	// GetPDBsForPods will return a map indexed by podnames
	// with associated PDBs as value
	GetPDBsForPods(ctx context.Context, pods []*corev1.Pod) (map[string][]*policyv1.PodDisruptionBudget, error)
}

func (i *Indexer) GetPDBsBlockedByPod(ctx context.Context, podName, ns string) ([]*policyv1.PodDisruptionBudget, error) {
	key := GeneratePodIndexKey(podName, ns)
	return GetFromIndex[policyv1.PodDisruptionBudget](ctx, i, PDBBlockByPodIdx, key)
}

func (i *Indexer) GetPDBsForPods(ctx context.Context, pods []*corev1.Pod) (map[string][]*policyv1.PodDisruptionBudget, error) {
	result := map[string][]*policyv1.PodDisruptionBudget{}

	// let's group pods per namespace
	// to perform analysis per namespace
	perNamespace := map[string][]*corev1.Pod{}
	for _, p := range pods {
		perNs := perNamespace[p.Namespace]
		perNamespace[p.Namespace] = append(perNs, p)
	}

	// work for each namespace
	for ns, podsInNs := range perNamespace {
		var pdbList policyv1.PodDisruptionBudgetList
		if err := i.client.List(ctx, &pdbList, &clientcr.ListOptions{Namespace: ns}); err != nil {
			// TODO log ? missing logger in indexer
			continue
		}
		// local cache for pdb selector (building selector can be expensive)
		pdbSelectors := map[*policyv1.PodDisruptionBudget]labels.Selector{}
		getSelector := func(pdb *policyv1.PodDisruptionBudget) (labels.Selector, bool) {
			if s, ok := pdbSelectors[pdb]; ok {
				return s, true
			}
			selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
			if err != nil {
				return nil, false
			}
			pdbSelectors[pdb] = selector
			return selector, true
		}

		for _, pod := range podsInNs {
			ls := labels.Set(pod.GetLabels())
			for i := range pdbList.Items {
				pdb := &pdbList.Items[i]
				selector, ok := getSelector(pdb)
				if !ok {
					// TODO log ? missing logger in indexer
					continue
				}

				if !selector.Matches(ls) {
					continue
				}
				podKey := GeneratePodIndexKey(pod.Name, pod.Namespace)
				otherPDBs := result[podKey]
				result[podKey] = append(otherPDBs, pdb)
			}
		}
	}
	return result, nil
}
func initPDBIndexer(client clientcr.Client, cache cachecr.Cache) error {
	informer, err := cache.GetInformer(context.Background(), &policyv1.PodDisruptionBudget{})
	if err != nil {
		return err
	}
	return informer.AddIndexers(map[string]cachek.IndexFunc{
		PDBBlockByPodIdx: func(obj interface{}) ([]string, error) { return indexPDBBlockingPod(client, obj) },
	})
}

func indexPDBBlockingPod(client clientcr.Client, o interface{}) ([]string, error) {
	pdb, ok := o.(*policyv1.PodDisruptionBudget)
	if !ok {
		return nil, errors.New("cannot parse pdb object in indexer")
	}
	return getBlockingPodsForPDB(client, pdb)
}

func getBlockingPodsForPDB(client clientcr.Client, pdb *policyv1.PodDisruptionBudget) ([]string, error) {
	var pods corev1.PodList
	if err := client.List(context.Background(), &pods, &clientcr.ListOptions{Namespace: pdb.GetNamespace()}); err != nil {
		return []string{}, err
	}

	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return []string{}, err
	}

	blockingPods := make([]string, 0)
	for _, pod := range pods.Items {
		if utils.IsPodReady(&pod) {
			continue
		}

		ls := labels.Set(pod.GetLabels())
		if !selector.Matches(ls) {
			continue
		}

		blockingPods = append(blockingPods, GeneratePodIndexKey(pod.GetName(), pod.GetNamespace()))
	}
	return blockingPods, nil
}

func GeneratePodIndexKey(podName, ns string) string {
	// This is needed because PDBs are namespace scoped, so we might have name collisions
	return fmt.Sprintf("%s/%s", podName, ns) // TODO check with Daniel: why in this order ?
}
