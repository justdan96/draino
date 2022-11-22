package drain

import (
	"context"
	"fmt"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DrainSimulator interface {
	SimulateDrain(context.Context, *corev1.Pod) (bool, error)
}

type drainSimulatorImpl struct {
	store      kubernetes.RuntimeObjectStore
	pdbIndexer index.PDBIndexer
	podIndexer index.PodIndexer
	client     client.Client

	podResultCache utils.TTLCache[bool]
}

var _ DrainSimulator = &drainSimulatorImpl{}

func NewDrainSimulator(client client.Client, indexer *index.Indexer, store kubernetes.RuntimeObjectStore) DrainSimulator {
	simulator := &drainSimulatorImpl{
		store:      store,
		podIndexer: indexer,
		pdbIndexer: indexer,
		client:     client,

		podResultCache: utils.NewCache[bool](3 * time.Minute),
	}
	go simulator.podResultCache.StartCleanupLoop(context.Background())
	return simulator
}

func (sim *drainSimulatorImpl) SimulateDrain(ctx context.Context, pod *corev1.Pod) (bool, error) {
	// check if eviction++
	_, ok := kubernetes.GetAnnotationFromPodOrController(kubernetes.EvictionAPIURLAnnotationKey, pod, sim.store)
	if ok {
		return true, nil
	}

	// check amount of pdbs the pod is associated
	pdbs, err := sim.pdbIndexer.GetPDBsForPods(ctx, []*corev1.Pod{pod})
	if err != nil {
		return false, err
	}

	podKey := index.GeneratePodIndexKey(pod.GetName(), pod.GetNamespace())
	if len(pdbs[podKey]) > 1 {
		return false, fmt.Errorf("Pod has more than one associated PDB %d > 1", len(pdbs))
	}

	// check if pod is blocking pdbs
	if len(pdbs[podKey]) == 1 {
		pdb := pdbs[podKey][0]
		if pdb.Status.DisruptionsAllowed < 1 {
			return false, fmt.Errorf("PDB '%s' does not allow any disruptions", pdb.GetName())
		}
	}

	// do a dry-run eviction call
	evictionDryRunRes := sim.simulateAPIEviction(ctx, pod)
	if !evictionDryRunRes {
		return false, fmt.Errorf("Eviction dry run was not successful")
	}

	return true, nil
}

func (sim *drainSimulatorImpl) simulateAPIEviction(ctx context.Context, pod *corev1.Pod) bool {
	if result, exist := sim.podResultCache.Get(getPodCacheIdx(pod)); exist {
		return result
	}

	var gracePeriod int64 = 30
	err := sim.client.Create(ctx, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.GetName(),
			Namespace: pod.GetNamespace(),
		},
		DeleteOptions: &metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
			DryRun:             []string{"all"},
		},
	})

	isEvictionAllowed := err != nil
	sim.podResultCache.Add(getPodCacheIdx(pod), isEvictionAllowed)

	return isEvictionAllowed
}

func getPodCacheIdx(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())
}
