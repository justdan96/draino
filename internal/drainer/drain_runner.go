package drainer

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ groups.Runner = &drainRunner{}

// drainRunner implements the groups.Runner interface and will be used to drain nodes of the given group configuration
type drainRunner struct {
	client client.Client
	logger logr.Logger
	clock  clock.Clock
	conf   *DrainRunnerConfig
}

func (runner *drainRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)
	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		candidates, err := runner.getDrainCandidates(info.Key)
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
			if err := runner.drainCandidate(info.Context, &candidate); err != nil {
				runner.logger.Error(err, "error during candidate evaluation", "node_name", candidate.Name)
			}
		}
	}, runner.conf.rerunEvery)
	return nil
}

func (runner *drainRunner) drainCandidate(ctx context.Context, candidate *corev1.Node) error {
	// check if the candidate is in the loop for too long
	if candidateReachedTimeout(runner.conf.drainTimeout, candidate, runner.clock.Now()) {
		runner.logger.Error(errors.New("candidate reached drain timeout"), "will remove candidate status from node", "node_name", candidate.Name)
		return runner.removeDrainCandidate(candidate)
	}

	// Add a taint to the node, so that no new poods are scheduled during the draining
	if err := runner.taintNode(candidate); err != nil {
		return err
	}

	allPreprocessorsDone := true
	for _, pre := range runner.conf.preprocessors {
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
	return nil
}

func (runner *drainRunner) getDrainCandidates(key groups.GroupKey) ([]corev1.Node, error) {
	// TODO get all nodes (of this group) and filter the ones that are marked to be drained
	// Maybe use an index?
	return []corev1.Node{}, nil
}

func (runner *drainRunner) removeDrainCandidate(candidate *corev1.Node) error {
	return runner.untaintNode(candidate)
}

func (runner *drainRunner) taintNode(candidate *corev1.Node) error {
	taint := createTaint(runner.clock.Now())
	newNode, updated, err := taints.AddOrUpdateTaint(candidate, taint)
	if err != nil {
		return err
	}
	if !updated {
		return nil
	}
	return runner.client.Update(context.Background(), newNode)
}

func (runner *drainRunner) untaintNode(candidate *corev1.Node) error {
	taint := createTaint(runner.clock.Now())
	newNode, updated, err := taints.RemoveTaint(candidate, taint)
	if err != nil {
		return err
	}
	if !updated {
		return nil
	}
	return runner.client.Update(context.Background(), newNode)
}

func createTaint(now time.Time) *corev1.Taint {
	timeAdded := v1.NewTime(now)
	taint := corev1.Taint{Key: "draino", Value: "drain-candidate", Effect: corev1.TaintEffectNoSchedule, TimeAdded: &timeAdded}
	return &taint
}

func candidateReachedTimeout(timeout time.Duration, candidate *corev1.Node, now time.Time) bool {
	// TODO get drain start time
	drainStart := candidate.CreationTimestamp.Time
	return drainStart.Add(timeout).After(now)
}
