package pre_processor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
)

const (
	PreActivityAnnotationPrefix     = "node-lifecycle.datadoghq.com/pre-activity-"
	PreActivityAnnotationNotStarted = ""
	PreActivityAnnotationProcessing = "processing"
	PreActivityAnnotationDone       = "done"
	PreActivityAnnotationFailed     = "failed"

	PreActivityTimeoutAnnotationPrefix = "node-lifecycle.datadoghq.com/timeout-pre-activity-"

	eventPreActivityBadConfiguration = "PreActivityBadConfiguration"
	eventPreActivityFailed           = "PreActivityFailed"
)

const PreProcessNotDoneReasonNotCandidate PreProcessNotDoneReason = "given node is not a candidate"

// PreActivitiesPreProcessor is checking if all the pre activities, set on nodes/pods/controller, are done.
// https://datadoghq.atlassian.net/wiki/spaces/K8S/pages/2791706268/WIP+NLA+Pre-activies+API+proposal
type PreActivitiesPreProcessor struct {
	podIndexer     index.PodIndexer
	store          kubernetes.RuntimeObjectStore
	logger         logr.Logger
	eventRecorder  kubernetes.EventRecorder
	clock          clock.Clock
	defaultTimeout time.Duration
}

func NewPreActivitiesPreProcessor(podIndexer index.PodIndexer, store kubernetes.RuntimeObjectStore, logger logr.Logger, eventRecorder kubernetes.EventRecorder, clock clock.Clock, defaultTimeout time.Duration) DrainPreProcessor {
	return &PreActivitiesPreProcessor{
		podIndexer:     podIndexer,
		store:          store,
		logger:         logger.WithName("PreActivitiesPreProcessor"),
		eventRecorder:  eventRecorder,
		clock:          clock,
		defaultTimeout: defaultTimeout,
	}
}

func (_ *PreActivitiesPreProcessor) GetName() string {
	return "PreActivitiesPreProcessor"
}

func preActivityStateConverter(val string) (string, error) {
	switch val {
	case PreActivityAnnotationNotStarted, PreActivityAnnotationProcessing, PreActivityAnnotationFailed, PreActivityAnnotationDone:
		return val, nil
	}
	return "", fmt.Errorf("Invalid pre activity value: %s", val)
}

func (pre *PreActivitiesPreProcessor) IsDone(ctx context.Context, node *corev1.Node) (bool, PreProcessNotDoneReason, error) {
	activities, err := pre.getActivities(ctx, node)
	if err != nil {
		return false, "", err
	}

	taint, exist := k8sclient.GetNLATaint(node)
	if !exist {
		return false, PreProcessNotDoneReasonNotCandidate, nil
	}

	candidateSince := taint.TimeAdded.Time
	for key, entry := range activities {
		logger := pre.logger.WithValues("node", node.Name, "annotation", key, "state", entry.state, "timeout", entry.timeout)

		switch entry.state {
		case PreActivityAnnotationDone:
			logger.V(logs.ZapDebug).Info("Pre activity finished successfully")
			continue
		case PreActivityAnnotationFailed:
			logger.Info("pre activity failed")
			pre.eventRecorder.NodeEventf(ctx, node, eventPreActivityFailed, "pre activity '%s' failed", key)
			return false, PreProcessNotDoneReasonFailure, nil
		default:
			if pre.clock.Now().Sub(candidateSince) > entry.timeout {
				logger.Info("pre activity timed out")
				pre.eventRecorder.NodeEventf(ctx, node, eventPreActivityFailed, "pre activity '%s' timed out", key)
				return false, PreProcessNotDoneReasonTimeout, nil
			}
			logger.V(logs.ZapDebug).Info("Waiting for pre activity to finish")
			return false, PreProcessNotDoneReasonProcessing, nil
		}
	}

	return true, "", nil
}

func (pre *PreActivitiesPreProcessor) Reset(ctx context.Context, node *corev1.Node) error {
	return nil
}

type preActivityConfiguration struct {
	state   string
	timeout time.Duration
}

// getActivities will search for all pre activity annotations in the whole chain (node -> pod -> controller).
// Furthermore, it's going to evaluate the timeout annotation for the same activity
func (pre *PreActivitiesPreProcessor) getActivities(ctx context.Context, node *corev1.Node) (map[string]*preActivityConfiguration, error) {
	activitySearch, err := kubernetes.NewSearch(ctx, pre.podIndexer, pre.store, preActivityStateConverter, node, PreActivityAnnotationPrefix, false, false, kubernetes.GetPrefixedAnnotation)
	if err != nil {
		return nil, err
	}
	activityTimeoutSearch, err := kubernetes.NewSearch(ctx, pre.podIndexer, pre.store, time.ParseDuration, node, PreActivityTimeoutAnnotationPrefix, false, false, kubernetes.GetPrefixedAnnotation)
	if err != nil {
		return nil, err
	}

	activitySearch.HandlerError(
		func(n *corev1.Node, err error) {
			pre.eventRecorder.NodeEventf(ctx, n, corev1.EventTypeWarning, eventPreActivityBadConfiguration, "invalid pre activity state: %v", err)
		},
		func(p *corev1.Pod, err error) {
			pre.eventRecorder.PodEventf(ctx, p, corev1.EventTypeWarning, eventPreActivityBadConfiguration, "invalid pre activity state: %v", err)
		},
	)

	activityTimeoutSearch.HandlerError(
		func(n *corev1.Node, err error) {
			pre.eventRecorder.NodeEventf(ctx, n, corev1.EventTypeWarning, eventPreActivityBadConfiguration, "failed to parse pre activity timeout: "+err.Error()) // The parsing error is given to the user
		},
		func(p *corev1.Pod, err error) {
			pre.eventRecorder.PodEventf(ctx, p, corev1.EventTypeWarning, eventPreActivityBadConfiguration, "failed to parse pre activity timeout: "+err.Error()) // The parsing error is given to the user
		},
	)

	result := map[string]*preActivityConfiguration{}
	for _, item := range activitySearch.Results() {
		key := keyFromMetadataSearchResultItem(item, PreActivityAnnotationPrefix)
		result[key] = &preActivityConfiguration{state: item.Value, timeout: pre.defaultTimeout}
	}

	for _, item := range activityTimeoutSearch.Results() {
		key := keyFromMetadataSearchResultItem(item, PreActivityTimeoutAnnotationPrefix)
		if _, exist := result[key]; !exist {
			// TODO log event to the user
			pre.logger.Info("found pre activity timeout without corresponding state annotation", "annotation", item.Key, "object_id", item.GetItemId(), "node", node.Name)
			continue
		}
		result[key].timeout = item.Value
	}

	return result, nil
}

// generates a unique key for the given result item, based on the found condition
func keyFromMetadataSearchResultItem[T any](item kubernetes.MetadataSearchResultItem[T], prefix string) string {
	preActivityName := strings.ReplaceAll(item.Key, prefix, "")
	return fmt.Sprintf("%s/%s", item.GetItemId(), preActivityName)
}
