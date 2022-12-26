package candidate_runner

import (
	"context"
	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/protector"
	"github.com/planetlabs/draino/internal/scheduler"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Make sure that the drain runner is implementing the group runner interface
var _ groups.Runner = &candidateRunner{}

type NodeSorters []scheduler.LessFunc[*corev1.Node]
type NodeIteratorFactory func([]*corev1.Node, NodeSorters) scheduler.ItemProvider[*corev1.Node]

type FilterFactory func(group string) filters.Filter

// candidateRunner implements the groups.Runner interface and will be used to drain nodes of the given group configuration
type candidateRunner struct {
	client              client.Client
	logger              logr.Logger
	clock               clock.Clock
	sharedIndexInformer index.GetSharedIndexInformer
	runEvery            time.Duration
	eventRecorder       kubernetes.EventRecorder
	pvProtector         protector.PVProtector

	filterFactory FilterFactory

	maxSimultaneousCandidates int
	dryRun                    bool

	nodeSorters         NodeSorters
	nodeIteratorFactory NodeIteratorFactory
	drainSimulator      drain.DrainSimulator
}

func (runner *candidateRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)

	filter := runner.filterFactory(string(info.Key))

	// TODO if we add metrics associated with that key, when the group is closed we should purge all the series associated with that key (cleanup-gauges with groupKey=...)?
	runner.logger = runner.logger.WithValues("groupKey", info.Key)
	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		nodes, err := index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(info.Key))
		// in case of an error we'll just try it again
		if err != nil {
			runner.logger.Error(err, "cannot get nodes for group")
			return
		}

		// TODO add metric to track amount of nodes in the group
		if len(nodes) == 0 {
			// If there are no candidates left, we'll stop the loop
			runner.logger.Info("no nodes in group left, stopping.")
			cancel()
			return
		}

		// filter nodes that are already candidate
		nodes, alreadyCandidateNodes, maxReached := runner.checkAlreadyCandidates(nodes)
		if maxReached {
			runner.logger.Info("Max candidate already reached", "count", runner.maxSimultaneousCandidates, "nodes", strings.Join(utils.NodesNames(alreadyCandidateNodes), ","))
			return
		}
		remainCandidateSlot := runner.maxSimultaneousCandidates - len(alreadyCandidateNodes)

		nodes = filter.Filter(nodes)

		nodeProvider := runner.nodeIteratorFactory(nodes, runner.nodeSorters)
		for node, ok := nodeProvider.Next(); ok && remainCandidateSlot > 0; node, ok = nodeProvider.Next() {
			logForNode := runner.logger.WithValues("node", node.Name)
			// check that the node can be drained
			canDrain, reasons, errDrainSimulation := runner.drainSimulator.SimulateDrain(ctx, node)
			if errDrainSimulation != nil {
				logForNode.Error(errDrainSimulation, "Failed to simulate drain")
				continue
			}
			if !canDrain {
				logForNode.Info("Rejected by drain simulation", "reason", strings.Join(reasons, ";"))
				continue
			}

			logForNode.Info("Adding drain candidate taint")
			if !runner.dryRun {

				if blockingPods, errPvProtection := runner.pvProtector.GetUnscheduledPodsBoundToNodeByPV(node); errPvProtection != nil {
					logForNode.Error(err, "Failed to run PV protection")
					continue
				} else if len(blockingPods) > 0 {
					kubernetes.LogrForVerboseNode(runner.logger, node, "Node can't become drain candidate: Pod needs to be scheduled on node due to PV binding", "pod", blockingPods[0].Namespace+"/"+blockingPods[0].Name)
					continue
				}

				if _, errTaint := k8sclient.AddNLATaint(ctx, runner.client, node, runner.clock.Now(), k8sclient.TaintDrainCandidate); errTaint != nil {
					logForNode.Error(errTaint, "Failed to taint node")
					continue // let's try next node, maybe this one has a problem
				}
			}
			remainCandidateSlot--
		}
		runner.logger.Info("Remain slot after drain candidate analysis", "count", remainCandidateSlot)

	}, runner.runEvery)
	return nil
}

// checkAlreadyCandidates keep only the nodes that are not candidate. If maxSimultaneousCandidates>0, then we check against the max. If max is reached a nil slice is returned and the boolean returned is true
func (runner *candidateRunner) checkAlreadyCandidates(nodes []*corev1.Node) (remainingNodes, alreadyCandidateNodes []*corev1.Node, maxCandidateReached bool) {
	remainingNodes = make([]*corev1.Node, 0, len(nodes)) // high probability that all nodes are to be kept
	alreadyCandidateNodes = make([]*corev1.Node, 0, runner.maxSimultaneousCandidates)
	for _, n := range nodes {
		if _, hasTaint := k8sclient.GetNLATaint(n); !hasTaint {
			remainingNodes = append(remainingNodes, n)
		} else {
			alreadyCandidateNodes = append(alreadyCandidateNodes, n)
			if runner.maxSimultaneousCandidates > 0 {
				if len(alreadyCandidateNodes) >= runner.maxSimultaneousCandidates {
					return nil, alreadyCandidateNodes, true
				}
			}
		}
	}
	return remainingNodes, alreadyCandidateNodes, false
}
