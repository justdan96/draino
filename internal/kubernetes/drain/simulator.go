package drain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DrainSimulator interface {
	// SimulateDrain will simulate a drain for the given node.
	// This means that it will perform an eviction simulation of all pods running on the node.
	SimulateDrain(context.Context, *corev1.Node) (bool, error)
	// SimulatePodDrain will simulate a drain of the given pod.
	// Before calling the API server it will make sure that some of the obvious problems are not given.
	SimulatePodDrain(context.Context, *corev1.Pod) (bool, error)
}

type drainSimulatorImpl struct {
	store      kubernetes.RuntimeObjectStore
	pdbIndexer index.PDBIndexer
	podIndexer index.PodIndexer
	client     client.Client
	// podFilter will be used to evaluate if pods running on a node should go through the eviction simulation
	podFilter kubernetes.PodFilterFunc

	nodeResultCache utils.TTLCache[simulationResult]
	podResultCache  utils.TTLCache[simulationResult]
}

type simulationResult struct {
	result bool
	reason string
}

var _ DrainSimulator = &drainSimulatorImpl{}

func NewDrainSimulator(
	ctx context.Context,
	client client.Client,
	indexer *index.Indexer,
	store kubernetes.RuntimeObjectStore,
	podFilter kubernetes.PodFilterFunc,
) DrainSimulator {
	simulator := &drainSimulatorImpl{
		store:      store,
		podIndexer: indexer,
		pdbIndexer: indexer,
		client:     client,
		podFilter:  podFilter,

		// TODO i'm not very sure if both of the caches make sense, as they will cache the same result
		nodeResultCache: utils.NewTTLCache[simulationResult](3*time.Minute, 10*time.Second),
		podResultCache:  utils.NewTTLCache[simulationResult](3*time.Minute, 10*time.Second),
	}

	go simulator.nodeResultCache.StartCleanupLoop(ctx)
	go simulator.podResultCache.StartCleanupLoop(ctx)

	return simulator
}

func (sim *drainSimulatorImpl) SimulateDrain(ctx context.Context, node *corev1.Node) (bool, error) {
	if result, exist := sim.nodeResultCache.Get(string(node.GetUID())); exist {
		return result.result, errors.New(result.reason)
	}

	pods, err := sim.podIndexer.GetPodsByNode(ctx, node.GetName())
	if err != nil {
		return false, err
	}

	reasons := []string{}
	for _, pod := range pods {
		// TODO handle error
		passes, _, _ := sim.podFilter(*pod)
		if !passes {
			continue
		}

		if result, exist := sim.podResultCache.Get(string(pod.GetUID())); exist {
			if !result.result {
				reasons = append(reasons, result.reason)
			}
			continue
		}

		if res, err := sim.SimulatePodDrain(ctx, pod); err != nil {
			// TODO add suceeded/failed pod drain simulation count metric
			reason := fmt.Sprintf("Cannot drain pod '%s', because: %v", pod.GetName(), err)
			reasons = append(reasons, reason)
			sim.podResultCache.Add(string(pod.GetUID()), simulationResult{result: res, reason: reason})
		}
	}

	resonString := strings.Join(reasons, "; ")
	sim.nodeResultCache.Add(
		string(node.GetUID()),
		simulationResult{result: len(reasons) == 0, reason: resonString},
	)

	// TODO add suceeded/failed node drain simulation count metric
	if len(reasons) > 0 {
		return false, errors.New(resonString)
	}

	return true, nil
}

func (sim *drainSimulatorImpl) SimulatePodDrain(ctx context.Context, pod *corev1.Pod) (bool, error) {
	// TODO should we check if the given pod will be accepted by the filter?

	// If eviction++ is enabled for the pod or controller, we don't want to perform any actions as there might not be any dry-run mode.
	// We'll just assume a successful simulation as we can neither know that it will succeed nor that it'll fail, so we have to try it.
	_, ok := kubernetes.GetAnnotationFromPodOrController(kubernetes.EvictionAPIURLAnnotationKey, pod, sim.store)
	if ok {
		return true, nil
	}

	pdbs, err := sim.pdbIndexer.GetPDBsForPods(ctx, []*corev1.Pod{pod})
	if err != nil {
		return false, err
	}

	// If there is more than one PDB associated to the given pod, the eviction will fail for sure due to the APIServer behaviour.
	podKey := index.GeneratePodIndexKey(pod.GetName(), pod.GetNamespace())
	if len(pdbs[podKey]) > 1 {
		return false, fmt.Errorf("Pod has more than one associated PDB %d > 1", len(pdbs[podKey]))
	}

	// If there is a matching PDB, check if it would allow disruptions
	if len(pdbs[podKey]) == 1 {
		pdb := pdbs[podKey][0]
		if analyser.IsPDBBlocked(ctx, pod, pdb) {
			return false, fmt.Errorf("PDB '%s' does not allow any disruptions", pdb.GetName())
		}
	}

	// do a dry-run eviction call
	evictionDryRunRes, err := sim.simulateAPIEviction(ctx, pod)
	if !evictionDryRunRes {
		return false, fmt.Errorf("Eviction dry run was not successful: %v", err)
	}

	return true, nil
}

func (sim *drainSimulatorImpl) simulateAPIEviction(ctx context.Context, pod *corev1.Pod) (bool, error) {
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

	return err == nil, err
}
