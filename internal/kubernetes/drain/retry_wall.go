package drain

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TODO think about proper annotation name
const RetryWallCountAnnotation = "draino/retry-attempt"

type RetryWall interface {
	RegisterRetryStrategies(...RetryStrategy)
	GetDelay(*corev1.Node) (delay time.Duration, canRetry bool, err error)
	NoteDrainFailure(*corev1.Node) error
	ResetRetryCount(*corev1.Node) error
}

type retryWallImpl struct {
	logger          logr.Logger
	client          client.Client
	defaultStrategy RetryStrategy
	strategies      map[string]RetryStrategy
}

var _ RetryWall = &retryWallImpl{}

func NewRetryWall(client client.Client, logger logr.Logger, strategies ...RetryStrategy) RetryWall {
	wall := &retryWallImpl{
		client:     client,
		logger:     logger,
		strategies: map[string]RetryStrategy{},
	}
	wall.RegisterRetryStrategies(strategies...)
	return wall
}

func (wall *retryWallImpl) RegisterRetryStrategies(strategies ...RetryStrategy) {
	if len(strategies) == 0 {
		return
	}
	if wall.defaultStrategy == nil {
		wall.defaultStrategy = strategies[0]
	}
	for _, strategy := range strategies {
		wall.strategies[strategy.GetName()] = strategy
	}
}

func (wall *retryWallImpl) GetDelay(node *corev1.Node) (time.Duration, bool, error) {
	retries, err := wall.getRetryCount(node)
	if err != nil {
		// TODO does this make sense? Theoretically getRetryCount only fails if the number in the annotation is not parseable.
		wall.logger.Error(err, "unable to get retry wall count from node", "node", node.GetName(), "annotations", node.GetAnnotations())
		retries = 0
	}

	// if this is the first try, we should not inject any delay
	if retries == 0 {
		return 0, true, nil
	}

	strategy, err := wall.getStrategyFromNode(node)
	if err != nil {
		return 0, false, err
	}

	if retries >= strategy.GetMaxRetries() {
		return 0, false, nil
	}

	return strategy.GetDuration(retries), true, nil
}

func (wall *retryWallImpl) getStrategyFromNode(node *corev1.Node) (RetryStrategy, error) {
	// TODO: here we can check the node annotations and find the related strategy
	// TODO log error as event?
	nodeAnnotationStrategy, useDefault, err := BuildNodeAnnotationRetryStrategy(node, wall.defaultStrategy)
	// for now we are using the default strategy in case of an error
	if err == nil && !useDefault {
		return nodeAnnotationStrategy, nil
	}

	return wall.defaultStrategy, nil
}

func (wall *retryWallImpl) NoteDrainFailure(node *corev1.Node) error {
	retryCount, err := wall.getRetryCount(node)
	if err != nil {
		wall.logger.Error(err, "unable to get retry wall count from node", "node", node.GetName(), "annotations", node.GetAnnotations())
		retryCount = 0
	}

	retryCount += 1
	return wall.patchRetryCountOnNode(node, retryCount)
}

func (wall *retryWallImpl) ResetRetryCount(node *corev1.Node) error {
	return wall.patchRetryCountOnNode(node, 0)
}

func (wall *retryWallImpl) getRetryCount(node *corev1.Node) (int, error) {
	annoations := node.GetAnnotations()
	if annoations == nil {
		return 0, nil
	}
	annotationVal, ok := annoations[RetryWallCountAnnotation]
	if !ok {
		return 0, nil
	}

	intVal, err := strconv.ParseInt(annotationVal, 10, 32)
	if err != nil {
		return 0, err
	}

	return int(intVal), nil
}

func (wall *retryWallImpl) patchRetryCountOnNode(node *corev1.Node, retryCount int) error {
	if node.GetAnnotations() == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[RetryWallCountAnnotation] = fmt.Sprintf("%d", retryCount)
	return wall.client.Patch(context.Background(), node, &RetryCountPatch{})
}
