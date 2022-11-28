package drain

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// RetryWallConditionType the condition type used to save the drain failure count
	RetryWallConditionType corev1.NodeConditionType = "DrainFailure"
	// retryConditionMsgSeparator the separator used in the condition message to separate the count from the message
	retryConditionMsgSeparator string = "|"
)

// TimeDefault is used as default if there was no retry set
var TimeDefault = time.Now().Add(-1 * 24 * time.Hour)

type RetryWall interface {
	// SetNewRetryWallTimestamp increases the retry count on the given node with the given string
	// now is used for last heartbeat timestamp and should usually be time.Now()
	SetNewRetryWallTimestamp(ctx context.Context, node *corev1.Node, reason string, now time.Time) error
	// GetRetryWallTimestamp returns the next time the given node should be retried
	// If there was no drain failure yet, it will return a timestamp in the past.
	GetRetryWallTimestamp(*corev1.Node) time.Time
	// GetDrainRetryAttemptsCount returns the amount of drain failures recorded for the given node
	GetDrainRetryAttemptsCount(*corev1.Node) int
	// ResetRetryCount will reset the retry count of the given node to zero
	ResetRetryCount(context.Context, *corev1.Node) error
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
	wall.registerRetryStrategies(strategies...)

	return wall, nil
}

func (wall *retryWallImpl) registerRetryStrategies(strategies ...RetryStrategy) {
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

func (wall *retryWallImpl) SetNewRetryWallTimestamp(ctx context.Context, node *corev1.Node, reason string, now time.Time) error {
	retryCount, _, err := wall.getRetry(node)
	if err != nil {
		wall.logger.Error(err, "unable to get retry wall count from node", "node", node.GetName(), "conditions", node.Status.Conditions)
		retryCount = 0
	}

	retryCount += 1
	return wall.patchRetryCountOnNode(ctx, node, retryCount, reason, now)
}

func (wall *retryWallImpl) GetRetryWallTimestamp(node *corev1.Node) time.Time {
	retries, lastHeartbeatTime, err := wall.getRetry(node)
	if err != nil {
		wall.logger.Error(err, "unable to get retry wall information from node", "node", node.GetName(), "conditions", node.Status.Conditions)
		return TimeDefault
	}

	// if this is the first try, we should not inject any delay
	if retries == 0 {
		return TimeDefault
	}

	strategy := wall.getStrategyFromNode(node)
	if retries >= strategy.GetAlertThreashold() {
		wall.logger.Info("retry wall is hitting limit for node", "node_name", node.GetName(), "retry_strategy", strategy.GetName(), "retries", retries, "max_retries", strategy.GetAlertThreashold())
	}

	delay := strategy.GetDelay(retries)
	return lastHeartbeatTime.Add(delay)
}

func (wall *retryWallImpl) GetDrainRetryAttemptsCount(node *corev1.Node) int {
	retryCount, _, err := wall.getRetry(node)
	if err != nil {
		wall.logger.Error(err, "unable to get retry wall count from node", "node", node.GetName(), "conditions", node.Status.Conditions)
		return 0
	}
	return retryCount
}

func (wall *retryWallImpl) getStrategyFromNode(node *corev1.Node) RetryStrategy {
	// TODO: here we can check the node annotations and find the related strategy
	nodeAnnotationStrategy, err := BuildNodeAnnotationRetryStrategy(node, wall.defaultStrategy)
	if err != nil {
		wall.logger.Error(err, "node contains invalid retry wall configruation", "node_name", node.GetName(), "annotations", node.GetAnnotations())
	}

	return nodeAnnotationStrategy
}

func (wall *retryWallImpl) ResetRetryCount(ctx context.Context, node *corev1.Node) error {
	return wall.
		client.
		Status().
		Patch(ctx, node, &NodeConditionPatch{ConditionType: RetryWallConditionType, Operator: PatchOpRemove})
}

func (wall *retryWallImpl) getRetry(node *corev1.Node) (int, time.Time, error) {
	_, condition, found := utils.FindNodeCondition(RetryWallConditionType, node)
	if !found {
		return 0, TimeDefault, nil
	}

	split := strings.Split(condition.Message, retryConditionMsgSeparator)
	if len(split) != 2 {
		return 0, TimeDefault, fmt.Errorf("condition message is poorly configured")
	}

	val, err := strconv.ParseInt(split[0], 10, 32)
	if err != nil {
		return 0, TimeDefault, err
	}

	return int(val), condition.LastHeartbeatTime.Time, nil
}

func (wall *retryWallImpl) patchRetryCountOnNode(ctx context.Context, node *corev1.Node, retryCount int, reason string, now time.Time) error {
	operator := PatchOpReplace
	pos, _, found := utils.FindNodeCondition(RetryWallConditionType, node)
	if !found {
		operator = PatchOpAdd
		// we can use the length as the index as the append is done afterwards
		pos = len(node.Status.Conditions)
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type:               RetryWallConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(now),
			Reason:             "DrainRetryWall",
		})
	}

	node.Status.Conditions[pos].LastHeartbeatTime = metav1.NewTime(now)
	node.Status.Conditions[pos].Message = fmt.Sprintf("%d%s%s", retryCount, retryConditionMsgSeparator, reason)

	return wall.
		client.
		Status().
		Patch(ctx, node, &NodeConditionPatch{ConditionType: RetryWallConditionType, Operator: operator})
}
