package drain_runner

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ groups.Runner = &drainRunner{}

// drainRunner implements the groups.Runner interface and will be used to drain nodes of the given group configuration
type drainRunner struct {
	client              client.Client
	logger              logr.Logger
	clock               clock.Clock
	retryWall           drain.RetryWall
	drainer             kubernetes.Drainer
	sharedIndexInformer index.GetSharedIndexInformer
	runEvery            time.Duration

	preprocessors []DrainPreprozessor
}

func (runner *drainRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)
	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		candidates, err := runner.getDrainCandidates(ctx, info.Key)
		// in case of an error we'll just try it again
		if err != nil {
			runner.logger.Error(err, "cannot get drain candidates for group", "group_key", info.Key)
			return
		}
		if len(candidates) == 0 {
			// If there are no candidates left, we'll stop the loop
			runner.logger.Info("no candidates in group left, stopping.", "group_key", info.Key)
			cancel()
			return
		}

		for _, candidate := range candidates {
			if err := runner.drainCandidate(info.Context, candidate); err != nil {
				runner.logger.Error(err, "error during candidate evaluation", "node_name", candidate.Name)
			}
		}
	}, runner.runEvery)
	return nil
}

func (runner *drainRunner) drainCandidate(ctx context.Context, candidate *corev1.Node) error {
	allPreprocessorsDone := true
	for _, pre := range runner.preprocessors {
		done, err := pre.Process(candidate)
		if err != nil {
			allPreprocessorsDone = false
			runner.logger.Error(err, "failed during preprocessor evaluation", "preprocessor", pre.GetName(), "node_name", candidate.Name)
			continue
		}
		if !done {
			runner.logger.Info("preprocessor still pending", "node_name", candidate.Name, "preprocessor", pre.GetName())
			allPreprocessorsDone = false
		}
	}

	if !allPreprocessorsDone {
		runner.logger.Info("waiting for preprocessors to be done before draining", "node_name", candidate.Name)
		return nil
	}

	runner.logger.Info("all preprocessors of candidate are done; will start draining", "node_name", candidate.Name)
	return runner.drain(ctx, candidate)
}

func (runner *drainRunner) drain(ctx context.Context, candidate *corev1.Node) error {
	if err := k8sclient.TaintNode(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDraining); err != nil {
		return err
	}

	err := runner.drainer.Drain(ctx, candidate)
	if err != nil {
		return runner.removeFailedCandidate(ctx, candidate, err.Error())
	}

	return k8sclient.TaintNode(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDrained)
}

func (runner *drainRunner) removeFailedCandidate(ctx context.Context, candidate *corev1.Node, reason string) error {
	err := runner.retryWall.SetNewRetryWallTimestamp(ctx, candidate, reason, runner.clock.Now())
	if err != nil {
		return err
	}

	err = k8sclient.UntaintNode(ctx, runner.client, candidate)
	if err != nil {
		return err
	}

	return nil
}

func (runner *drainRunner) getDrainCandidates(ctx context.Context, key groups.GroupKey) ([]*corev1.Node, error) {
	nodes, err := index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(key))
	if err != nil {
		return nil, err
	}

	candidates := make([]*corev1.Node, 0)
	for _, node := range nodes {
		taint, exist := k8sclient.GetTaint(node)
		// if the node doesn't have the draino taint, it should not be processed
		if !exist {
			continue
		}
		// if the taint value is "drained", draino is done and the node should be ignored
		if taint.Value == k8sclient.TaintDrained {
			continue
		}
		candidates = append(candidates, node.DeepCopy())
	}

	return candidates, nil
}
