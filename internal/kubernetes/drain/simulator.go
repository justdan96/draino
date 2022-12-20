package drain

import (
	"context"
	"fmt"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	kubernetes2 "k8s.io/client-go/kubernetes"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	PositiveCacheResTTL = time.Minute
	NegativeCacheResTTL = 3 * time.Minute
)

type DrainSimulator interface {
	// SimulateDrain will simulate a drain for the given node.
	// This means that it will perform an eviction simulation of all pods running on the node.
	SimulateDrain(context.Context, *corev1.Node) (canEvict bool, reasons []string, err error)
	// SimulatePodDrain will simulate a drain of the given pod.
	// Before calling the API server it will make sure that some of the obvious problems are not given.
	SimulatePodDrain(context.Context, *corev1.Pod) (canEvict bool, reason string, err error)
}

type drainSimulatorImpl struct {
	pdbIndexer index.PDBIndexer
	podIndexer index.PodIndexer
	client     kubernetes2.Interface
	// skipPodFilter will be used to evaluate if pods running on a node should go through the eviction simulation
	skipPodFilter  kubernetes.PodFilterFunc
	podResultCache utils.TTLCache[simulationResult]
}

type simulationResult struct {
	result bool
	reason string
}

var _ DrainSimulator = &drainSimulatorImpl{}

func NewDrainSimulator(
	ctx context.Context,
	client kubernetes2.Interface,
	indexer *index.Indexer,
	skipPodFilter kubernetes.PodFilterFunc,
) DrainSimulator {
	simulator := &drainSimulatorImpl{
		podIndexer:    indexer,
		pdbIndexer:    indexer,
		client:        client,
		skipPodFilter: skipPodFilter,

		// TODO think about using alternative solutions like a MRU cache
		podResultCache: utils.NewTTLCache[simulationResult](3*time.Minute, 10*time.Second),
	}

	go simulator.podResultCache.StartCleanupLoop(ctx)

	return simulator
}

func (sim *drainSimulatorImpl) SimulateDrain(ctx context.Context, node *corev1.Node) (bool, []string, error) {
	pods, err := sim.podIndexer.GetPodsByNode(ctx, node.GetName())
	if err != nil {
		return false, nil, err
	}

	// As we are  caching the positive results for one minute and negative ones for three minutes, we might make a lot of unneeded API calls
	// As an optimization we are iterating over all pods and check if at least one has a negative cache entry, before simulating the drain for all the pods.
	reasons := []string{}
	for _, pod := range pods {
		if res, exist := sim.podResultCache.Get(createCacheKey(pod), time.Now()); exist && !res.result {
			reasons = append(reasons, res.reason)
		}
	}
	if len(reasons) > 0 {
		return false, reasons, nil
	}

	for _, pod := range pods {
		// TODO add suceeded/failed pod drain simulation count metric
		canEvict, reason, err := sim.SimulatePodDrain(ctx, pod)
		if err != nil {
			return false, nil, err
		}
		if !canEvict {
			reasons = append(reasons, fmt.Sprintf("Cannot drain pod '%s', because: %v", pod.GetName(), reason))
		}
	}

	// TODO add suceeded/failed node drain simulation count metric
	if len(reasons) > 0 {
		return false, reasons, nil
	}

	return true, nil, nil
}

func (sim *drainSimulatorImpl) SimulatePodDrain(ctx context.Context, pod *corev1.Pod) (bool, string, error) {
	if res, exist := sim.podResultCache.Get(createCacheKey(pod), time.Now()); exist {
		return res.result, res.reason, nil
	}

	passes, reason, err := sim.skipPodFilter(*pod)
	if err != nil {
		return false, reason, err
	}
	if !passes {
		// If the pod does not pass the filter, it means that it will be accepted by default
		sim.writePodCache(pod, true, reason)
		return true, reason, nil
	}

	pdbs, err := sim.pdbIndexer.GetPDBsForPods(ctx, []*corev1.Pod{pod})
	if err != nil {
		return false, "", err
	}

	// If there is more than one PDB associated to the given pod, the eviction will fail for sure due to the APIServer behaviour.
	podKey := index.GeneratePodIndexKey(pod.GetName(), pod.GetNamespace())
	if len(pdbs[podKey]) > 1 {
		reason = fmt.Sprintf("Pod has more than one associated PDB %d > 1", len(pdbs[podKey]))
		sim.writePodCache(pod, false, reason)
		return false, reason, nil
	}

	// If there is a matching PDB, check if it would allow disruptions
	if len(pdbs[podKey]) == 1 {
		pdb := pdbs[podKey][0]
		if analyser.IsPDBBlocked(ctx, pod, pdb) {
			reason = fmt.Sprintf("PDB '%s' does not allow any disruptions", pdb.GetName())
			sim.writePodCache(pod, false, reason)
			return false, reason, nil
		}
	}

	// do a dry-run eviction call
	evictionDryRunRes, err := sim.simulateAPIEviction(ctx, pod)
	if !evictionDryRunRes {
		reason = fmt.Sprintf("Eviction dry run was not successful: %v", err)
		sim.writePodCache(pod, false, reason)
		return false, reason, nil
	}

	sim.writePodCache(pod, true, "")
	return true, "", nil
}

func (sim *drainSimulatorImpl) simulateAPIEviction(ctx context.Context, pod *corev1.Pod) (bool, error) {
	var gracePeriod int64 = 30
	err := sim.client.SubResource("eviction").Create(ctx, pod, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.GetName(),
			Namespace: pod.GetNamespace(),
		},
		DeleteOptions: &metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
			DryRun:             []string{"All"},
		},
	})
	if err1 == nil {
		//fmt.Println("eviction simulation succeeded for v1")
		return true, nil
	}
	err2 := sim.client.PolicyV1beta1().Evictions(pod.Namespace).Evict(ctx, &policyv1beta1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.GetName(),
			Namespace: pod.GetNamespace(),
		},
		DeleteOptions: &metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
			DryRun:             []string{"All"},
		},
	})
	if err2 == nil {
		//fmt.Println("eviction simulation succeeded for v1beta")
		return true, nil
	}
	return false, err2
}

func (sim *drainSimulatorImpl) writePodCache(pod *corev1.Pod, result bool, reason string) {
	ttl := NegativeCacheResTTL
	if result {
		ttl = PositiveCacheResTTL
	}
	sim.podResultCache.AddCustomTTL(createCacheKey(pod), simulationResult{result: result, reason: reason}, ttl)
}

func createCacheKey(pod *corev1.Pod) string {
	return string(pod.UID)
}
