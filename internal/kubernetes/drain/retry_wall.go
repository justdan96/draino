package drain

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const RetryWallConditionType corev1.NodeConditionType = "DrainFailure"

type RetryWall interface {
	// GetDelay will return a delay to be respected since the last drain try
	GetDelay(*corev1.Node) (delay time.Duration)
	// NoteDrainFailure will increase the retry-cont on the node by one
	NoteDrainFailure(*corev1.Node) error
	// ResetRetryCount will reset the retry count of the given node to zero
	ResetRetryCount(*corev1.Node) error
}

type retryWallImpl struct {
	logger logr.Logger
	client client.Client
	// the default strategy is the first one passed to RegisterRetryStrategies
	// it's also available in the strategies map
	defaultStrategy RetryStrategy
	strategies      map[string]RetryStrategy
}

var _ RetryWall = &retryWallImpl{}

// NewRetryWall will return a new instance of the retry wall
// It will return an error if no strategy was given
func NewRetryWall(client client.Client, logger logr.Logger, strategies ...RetryStrategy) (RetryWall, error) {
	if len(strategies) == 0 {
		return nil, fmt.Errorf("please provide at least one retry strategy to the retry wall, otherwise it will not work.")
	}

	wall := &retryWallImpl{
		client:     client,
		logger:     logger.WithName("retry-wall"),
		strategies: map[string]RetryStrategy{},
	}
	wall.RegisterRetryStrategies(strategies...)

	return wall, nil
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

func (wall *retryWallImpl) GetDelay(node *corev1.Node) time.Duration {
	retries, err := wall.getRetryCount(node)
	if err != nil {
		// TODO does this make sense? Theoretically getRetryCount only fails if the number in the annotation is not parseable.
		wall.logger.Error(err, "unable to get retry wall count from node", "node", node.GetName(), "annotations", node.GetAnnotations())
		retries = 0
	}

	// if this is the first try, we should not inject any delay
	if retries == 0 {
		return 0
	}

	strategy := wall.getStrategyFromNode(node)
	if retries >= strategy.GetAlertThreashold() {
		wall.logger.Info("retry wall is hitting limit for node", "node_name", node.GetName(), "retry_strategy", strategy.GetName(), "retries", retries, "max_retries", strategy.GetAlertThreashold())
	}

	return strategy.GetDelay(retries)
}

func (wall *retryWallImpl) getStrategyFromNode(node *corev1.Node) RetryStrategy {
	// TODO: here we can check the node annotations and find the related strategy
	// TODO log error as event?
	nodeAnnotationStrategy, useDefault, err := BuildNodeAnnotationRetryStrategy(node, wall.defaultStrategy)
	// for now we are using the default strategy in case of an error
	if err == nil && !useDefault {
		return nodeAnnotationStrategy
	}

	return wall.defaultStrategy
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
	return wall.
		client.
		Status().
		Patch(context.Background(), node, &NodeConditionPatch{ConditionType: RetryWallConditionType, Operator: PatchOpRemove})
}

func (wall *retryWallImpl) getRetryCount(node *corev1.Node) (int, error) {
	_, condition, found := utils.FindNodeCondition(RetryWallConditionType, node)
	if !found {
		return 0, nil
	}

	val, err := strconv.ParseInt(condition.Message, 10, 32)
	if err != nil {
		return 0, err
	}

	return int(val), nil
}

func (wall *retryWallImpl) patchRetryCountOnNode(node *corev1.Node, retryCount int) error {
	operator := PatchOpReplace
	pos, _, found := utils.FindNodeCondition(RetryWallConditionType, node)
	if !found {
		operator = PatchOpAdd
		// we can use the length as the index as the append is done afterwards
		pos = len(node.Status.Conditions)
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type:               RetryWallConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(time.Now()),
			LastHeartbeatTime:  metav1.NewTime(time.Now()),
			Reason:             "DrainRetryWall",
			Message:            fmt.Sprintf("%d", retryCount),
		})
	}

	node.Status.Conditions[pos].LastHeartbeatTime = metav1.NewTime(time.Now())
	node.Status.Conditions[pos].Message = fmt.Sprintf("%d", retryCount)

	return wall.
		client.
		Status().
		Patch(context.Background(), node, &NodeConditionPatch{ConditionType: RetryWallConditionType, Operator: operator})
}
