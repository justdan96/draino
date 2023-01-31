package kubernetes

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
)

const (
	ConfigurationLabelKey    = "node-lifecycle.datadoghq.com/draino-configuration"
	OutOfScopeLabelValue     = "out-of-scope"
	nodeOptionsMetricName    = "node_options_nodes_total"
	nodeOptionsCPUMetricName = "node_options_cpu_total"
)

type DrainoConfigurationObserver interface {
	Runner
	IsInScope(node *v1.Node) (bool, string, error)
	Reset()
}

var (
	MeasureNodeLabelPatchRateLimited = stats.Int64("draino/node_patch_ratelimited", "Number of rate limited patch label on nodes", stats.UnitDimensionless)
	MeasureNodeLabelPatchFailed      = stats.Int64("draino/node_patch_failed", "Number of failure while patching label on nodes", stats.UnitDimensionless)
)

// metricsObjectsForObserver groups all the object required to serve the metrics
type metricsObjectsForObserver struct {
	previousMeasureNodesWithNodeOptions *view.View
	MeasureNodesWithNodeOptions         *stats.Int64Measure

	previousMeasureCPUsWithNodeOptions *view.View
	MeasureCPUsWithNodeOptions         *stats.Int64Measure

	nodePatchRatelimitView *view.View
	nodePatchFailureView   *view.View
}

// initializeQueueMetrics initialize the metrics that are used to count internal retries and rateLimit
func (g *metricsObjectsForObserver) initializeQueueMetrics() {
	if g.nodePatchRatelimitView == nil {
		g.nodePatchRatelimitView = &view.View{
			Name:        "nodes_label_patch_ratelimited_total",
			Measure:     MeasureNodeLabelPatchRateLimited,
			Description: "Number of node label patch that are rate limited.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{},
		}
		view.Register(g.nodePatchRatelimitView)
	}
	if g.nodePatchFailureView == nil {
		g.nodePatchFailureView = &view.View{
			Name:        "nodes_label_patch_failed_total",
			Measure:     MeasureNodeLabelPatchFailed,
			Description: "Number of node label patch that have failed.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{},
		}
		view.Register(g.nodePatchFailureView)
	}
}

// reset: replace existing gauges to eliminate obsolete series
func (g *metricsObjectsForObserver) reset() error {
	if g.previousMeasureNodesWithNodeOptions != nil {
		view.Unregister(g.previousMeasureNodesWithNodeOptions)
	}
	if g.previousMeasureCPUsWithNodeOptions != nil {
		view.Unregister(g.previousMeasureCPUsWithNodeOptions)
	}

	if err := wait.Poll(100*time.Millisecond, 5*time.Second, func() (done bool, err error) { return view.Find(nodeOptionsMetricName) == nil, nil }); err != nil {
		return fmt.Errorf("failed to purge previous [node] series")
	} // wait for metrics engine to purge previous series

	if err := wait.Poll(100*time.Millisecond, 5*time.Second, func() (done bool, err error) { return view.Find(nodeOptionsCPUMetricName) == nil, nil }); err != nil {
		return fmt.Errorf("failed to purge previous [cpu] series")
	} // wait for metrics engine to purge previous series

	g.MeasureNodesWithNodeOptions = stats.Int64(nodeOptionsMetricName, "Number of nodes for each options", stats.UnitDimensionless)
	g.MeasureCPUsWithNodeOptions = stats.Int64(nodeOptionsCPUMetricName, "Number of cpu for each options", stats.UnitDimensionless)

	g.previousMeasureNodesWithNodeOptions = &view.View{
		Name:        nodeOptionsMetricName,
		Measure:     g.MeasureNodesWithNodeOptions,
		Description: "Number of nodes for each options",
		Aggregation: view.LastValue(),
		TagKeys: []tag.Key{
			TagNodegroupName,
			TagNodegroupNamePrefix,
			TagNodegroupNamespace,
			TagTeam,
			TagDrainStatus,
			TagConditions,
			TagUserOptInViaPodAnnotation,
			TagUserOptOutViaPodAnnotation,
			TagUserAllowedConditionsAnnotation,
			TagDrainRetry,
			TagDrainRetryFailed,
			TagDrainRetryCustomMaxAttempt,
			TagPVCManagement,
			TagPreprovisioning,
			TagInScope,
			TagUserEvictionURL,
			TagOverdue,
		},
	}

	g.previousMeasureCPUsWithNodeOptions = &view.View{
		Name:        nodeOptionsCPUMetricName,
		Measure:     g.MeasureCPUsWithNodeOptions,
		Description: "Number of cpu for each options",
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{TagNodegroupName, TagNodegroupNamePrefix, TagNodegroupNamespace, TagTeam, TagInScope, TagConditions},
	}

	view.Register(g.previousMeasureNodesWithNodeOptions)
	view.Register(g.previousMeasureCPUsWithNodeOptions)
	return nil
}

// DrainoConfigurationObserverImpl is responsible for annotating the nodes with the draino configuration that cover the node (if any)
// It also exposes a metrics 'draino_in_scope_nodes_total' that count the nodes of a nodegroup that are in scope of a given configuration. The metric is available per condition. A condition named 'any' is virtually defined to groups all the nodes (with or without conditions).
type DrainoConfigurationObserverImpl struct {
	kclient            client.Interface
	runtimeObjectStore RuntimeObjectStore
	analysisPeriod     time.Duration

	// queueNodeToBeUpdated: To avoid burst in node updates the work to be done is queued. This way we can pace the node updates.
	// The consequence is that the metric is not 100% accurate when the controller starts. It converges after couple ou cycles.
	queueNodeToBeUpdated workqueue.RateLimitingInterface
	nodePatchLimiter     flowcontrol.RateLimiter // client side protection for APIServer

	globalConfig        GlobalConfig
	nodeFilterFunc      func(obj interface{}) bool
	podFilterFunc       PodFilterFunc
	userOptOutPodFilter PodFilterFunc
	userOptInPodFilter  PodFilterFunc
	logger              *zap.Logger

	metricsObjects metricsObjectsForObserver
}

var _ DrainoConfigurationObserver = &DrainoConfigurationObserverImpl{}

func NewScopeObserver(client client.Interface, globalConfig GlobalConfig, runtimeObjectStore RuntimeObjectStore, analysisPeriod time.Duration, podFilterFunc, userOptInPodFilter, userOptOutPodFilter PodFilterFunc, nodeFilterFunc func(obj interface{}) bool, log *zap.Logger) DrainoConfigurationObserver {

	// We are not adding a BucketRateLimiter to that list because the same nodes are going to be appended periodically if the update fails
	// Failing nodes will already be in the queue with a retry. Added a BucketRL proved to be a problem here is the client side is not able to dequeue
	// faster that the add period. In that case the processing time for the item end up being pushed always further in the future and delay the processing more and more.
	// See: https://gist.github.com/dbenque/56c907adc1a1a1f08d16943cd390d7de
	//
	// For this reason I have preferred to move the bucket limiter on the other side of the queue, to have a kind of client-side protection on the API-Server.
	// See field nodePatchLimiter
	rateLimiters := []workqueue.RateLimiter{
		workqueue.NewItemExponentialFailureRateLimiter(500*time.Millisecond, 20*time.Second),
	}

	scopeObserver := &DrainoConfigurationObserverImpl{
		kclient:              client,
		runtimeObjectStore:   runtimeObjectStore,
		nodeFilterFunc:       nodeFilterFunc,
		podFilterFunc:        podFilterFunc,
		userOptOutPodFilter:  userOptOutPodFilter,
		userOptInPodFilter:   userOptInPodFilter,
		analysisPeriod:       analysisPeriod,
		logger:               log,
		globalConfig:         globalConfig,
		queueNodeToBeUpdated: workqueue.NewNamedRateLimitingQueue(workqueue.NewMaxOfRateLimiter(rateLimiters...), "nodeUpdater"),
		nodePatchLimiter:     flowcontrol.NewTokenBucketRateLimiter(50, 10), // client side protection
	}
	scopeObserver.metricsObjects.initializeQueueMetrics()

	return scopeObserver
}

type inScopeTags struct {
	NodeTagsValues
	DrainStatus                     string
	InScope                         bool
	PreprovisioningEnabled          bool
	PVCManagementEnabled            bool
	DrainRetry                      bool
	DrainRetryFailed                bool
	DrainRetryCustomMaxAttempts     bool
	UserOptOutViaPodAnnotation      bool
	UserOptInViaPodAnnotation       bool
	UserAllowedConditionsAnnotation bool
	TagUserEvictionURLViaAnnotation bool
	Condition                       string
	Overdue                         bool
}

type inScopeCPUTags struct {
	NodeTagsValues
	InScope   bool
	Condition string
}

type inScopeMetrics map[inScopeTags]int64
type inScopeCPUMetrics map[inScopeCPUTags]int64

func (s *DrainoConfigurationObserverImpl) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(s.analysisPeriod)
	// Wait for the informer to sync before starting
	wait.PollImmediateInfinite(10*time.Second, func() (done bool, err error) {
		return s.runtimeObjectStore.Pods().HasSynced(), nil
	})

	go s.processQueueForNodeUpdates()
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			s.queueNodeToBeUpdated.ShutDown()
			return
		case <-ticker.C:
			// Let's print the queue size
			s.logger.Info("queueNodeToBeUpdated", zap.Int("len", s.queueNodeToBeUpdated.Len()))

			// Let's update the nodes metadata
			for _, node := range s.runtimeObjectStore.Nodes().ListNodes() {
				_, outOfDate, err := s.getLabelUpdate(node)
				if err != nil {
					s.logger.Error("Failed to check if config annotation was out of date", zap.Error(err), zap.String("node", node.Name))
				} else if outOfDate {
					s.addNodeToQueue(node)
				}
			}
			newMetricsValue := inScopeMetrics{}
			newMetricsCPUValue := inScopeCPUMetrics{}
			// Let's update the metrics
			for _, node := range s.runtimeObjectStore.Nodes().ListNodes() {
				// skip the node if it is too recent... it does not have all the required labels/annotations yet to have relevant metrics
				if time.Now().Sub(node.CreationTimestamp.Time) < time.Minute {
					continue
				}

				nodeTags := GetNodeTagsValues(node)
				conditions := GetNodeOffendingConditions(node, s.globalConfig.SuppliedConditions)
				if node.Annotations == nil {
					node.Annotations = map[string]string{}
				}

				_, useDefaultRetryMaxAttempt, _ := GetNodeRetryMaxAttempt(node)

				t := inScopeTags{
					NodeTagsValues:                  nodeTags,
					DrainStatus:                     getDrainStatusStr(node),
					InScope:                         NodeInScopeWithConditionCheck(conditions, node),
					PreprovisioningEnabled:          node.Annotations[preprovisioningAnnotationKey] == preprovisioningAnnotationValue,
					PVCManagementEnabled:            s.HasPodWithPVCManagementEnabled(node),
					DrainRetry:                      DrainRetryEnabled(node),
					DrainRetryFailed:                HasDrainRetryFailedAnnotation(node),
					DrainRetryCustomMaxAttempts:     !useDefaultRetryMaxAttempt,
					UserOptOutViaPodAnnotation:      s.HasPodWithUserOptOutAnnotation(node),
					UserOptInViaPodAnnotation:       s.HasPodWithUserOptInAnnotation(node),
					UserAllowedConditionsAnnotation: hasAllowConditionList(node),
					TagUserEvictionURLViaAnnotation: s.HasEvictionUrlViaAnnotation(node),
				}

				tCPU := inScopeCPUTags{
					NodeTagsValues: nodeTags,
					InScope:        NodeInScopeWithConditionCheck(conditions, node),
				}

				overdue := map[string]bool{}
				for _, c := range conditions {
					overdue[string(c.Type)] = IsOverdue(node, c)
				}

				// adding a virtual condition 'any' to be able to count the nodes whatever the condition(s) or absence of condition.
				conditionsWithAll := append(GetConditionsTypes(conditions), "any")
				for _, c := range conditionsWithAll {
					t.Condition = c
					t.Overdue = overdue[c]
					newMetricsValue[t] = newMetricsValue[t] + 1

					tCPU.Condition = c
					newMetricsCPUValue[tCPU] = newMetricsCPUValue[tCPU] + node.Status.Capacity.Cpu().Value()

				}
			}
			s.updateGauges(newMetricsValue, newMetricsCPUValue)
		}
	}
}

func NodeInScopeWithConditionCheck(conditions []SuppliedCondition, node *v1.Node) bool {
	conditionsStr := GetConditionsTypes(conditions)
	return (len(node.Labels[ConfigurationLabelKey]) > 0 && node.Labels[ConfigurationLabelKey] != OutOfScopeLabelValue) && atLeastOneConditionAcceptedByTheNode(conditionsStr, node)
}

// updateGauges is in charge of updating the gauges values and purging the series that do not exist anymore
//
// Note: with opencensus unregistering/registering the view would clean-up the old series. The problem is that a metrics has to be registered to be recorded.
//
//	As a consequence there is a risk of concurrency between the goroutine that populates the fresh registered metric and the one that expose the metric for the scape.
//	There is no other way around I could find for the moment to cleanup old series. The concurrency risk is clearly acceptable if we look at the frequency of metric poll versus the frequency and a speed of metric generation.
//	In worst case the server will be missing series for a given scrape (not even report a bad value, just missing series). So the impact if it happens is insignificant.
func (s *DrainoConfigurationObserverImpl) updateGauges(metrics inScopeMetrics, metricsCPU inScopeCPUMetrics) {
	if err := s.metricsObjects.reset(); err != nil {
		s.logger.Error("Unable to purger previous metrics series")
		return
	}
	for tagsValues, count := range metrics {
		// This list of tags must be in sync with the list of tags in the function metricsObjectsForObserver::reset()
		allTags, _ := tag.New(context.Background(),
			tag.Upsert(TagNodegroupNamespace, tagsValues.NgNamespace), tag.Upsert(TagNodegroupName, tagsValues.NgName), tag.Upsert(TagNodegroupNamePrefix, GetNodeGroupNamePrefix(tagsValues.NgName)),
			tag.Upsert(TagTeam, tagsValues.Team),
			tag.Upsert(TagDrainStatus, tagsValues.DrainStatus),
			tag.Upsert(TagConditions, tagsValues.Condition),
			tag.Upsert(TagInScope, strconv.FormatBool(tagsValues.InScope)),
			tag.Upsert(TagPreprovisioning, strconv.FormatBool(tagsValues.PreprovisioningEnabled)),
			tag.Upsert(TagPVCManagement, strconv.FormatBool(tagsValues.PVCManagementEnabled)),
			tag.Upsert(TagDrainRetry, strconv.FormatBool(tagsValues.DrainRetry)),
			tag.Upsert(TagDrainRetryFailed, strconv.FormatBool(tagsValues.DrainRetryFailed)),
			tag.Upsert(TagDrainRetryCustomMaxAttempt, strconv.FormatBool(tagsValues.DrainRetryCustomMaxAttempts)),
			tag.Upsert(TagUserEvictionURL, strconv.FormatBool(tagsValues.TagUserEvictionURLViaAnnotation)),
			tag.Upsert(TagUserOptInViaPodAnnotation, strconv.FormatBool(tagsValues.UserOptInViaPodAnnotation)),
			tag.Upsert(TagUserOptOutViaPodAnnotation, strconv.FormatBool(tagsValues.UserOptOutViaPodAnnotation)),
			tag.Upsert(TagUserAllowedConditionsAnnotation, strconv.FormatBool(tagsValues.UserAllowedConditionsAnnotation)),
			tag.Upsert(TagOverdue, strconv.FormatBool(tagsValues.Overdue)))
		stats.Record(allTags, s.metricsObjects.MeasureNodesWithNodeOptions.M(count))
	}

	for tagsValues, count := range metricsCPU {
		// This list of tags must be in sync with the list of tags in the function metricsObjectsForObserver::reset()
		allTags, _ := tag.New(context.Background(),
			tag.Upsert(TagNodegroupNamespace, tagsValues.NgNamespace), tag.Upsert(TagNodegroupName, tagsValues.NgName), tag.Upsert(TagNodegroupNamePrefix, GetNodeGroupNamePrefix(tagsValues.NgName)),
			tag.Upsert(TagTeam, tagsValues.Team),
			tag.Upsert(TagConditions, tagsValues.Condition),
			tag.Upsert(TagInScope, strconv.FormatBool(tagsValues.InScope)))
		stats.Record(allTags, s.metricsObjects.MeasureCPUsWithNodeOptions.M(count))
	}
}

func (s *DrainoConfigurationObserverImpl) addNodeToQueue(node *v1.Node) {
	logFields := []zap.Field{zap.String("node", node.Name), zap.Int("requeue", s.queueNodeToBeUpdated.NumRequeues(node.Name))}
	s.logger.Info("Adding node to queue", logFields...)
	s.queueNodeToBeUpdated.AddRateLimited(node.Name)
}

// getLabelUpdate returns the label value the node should have and whether or not the label
// value is currently out of date (not equal to first return value)
func (s *DrainoConfigurationObserverImpl) getLabelUpdate(node *v1.Node) (string, bool, error) {
	valueOriginal := node.Labels[ConfigurationLabelKey]
	configsOriginal := strings.Split(valueOriginal, ".")
	var configs []string
	for _, config := range configsOriginal {
		if config == "" || config == OutOfScopeLabelValue || config == s.globalConfig.ConfigName {
			continue
		}
		configs = append(configs, config)
	}
	inScope, reason, err := s.IsInScope(node)
	if err != nil {
		return "", false, err
	}
	LogForVerboseNode(s.logger, node, "InScope information", zap.Bool("inScope", inScope), zap.String("reason", reason))
	if inScope {
		configs = append(configs, s.globalConfig.ConfigName)
	}
	if len(configs) == 0 {
		// add out of scope value for user visibility
		configs = append(configs, OutOfScopeLabelValue)
	}
	sort.Strings(configs)
	valueDesired := strings.Join(configs, ".")
	return valueDesired, valueDesired != valueOriginal, nil
}

// IsInScope return if the node is in scope of the running configuration. If not it also return the reason for not being in scope.
func (s *DrainoConfigurationObserverImpl) IsInScope(node *v1.Node) (inScope bool, reasonIfnOtInScope string, err error) {
	if !s.nodeFilterFunc(node) {
		return false, "labelSelection", nil
	}

	if hasLabel, enabled := IsNodeNLAEnableByLabel(node); hasLabel {
		if !enabled {
			return false, "Node label explicit opt-out", nil
		}
		return true, "", nil
	}

	var pods []*v1.Pod
	if pods, err = s.runtimeObjectStore.Pods().ListPodsForNode(node.Name); err != nil {
		return false, "", err
	}
	for _, p := range pods {
		passes, reason, err := s.podFilterFunc(*p)
		if err != nil {
			return false, "", err
		}
		if !passes {
			return false, reason, nil
		}
	}
	return true, "", nil
}

func (s *DrainoConfigurationObserverImpl) processQueueForNodeUpdates() {
	for {
		obj, shutdown := s.queueNodeToBeUpdated.Get()
		if shutdown {
			s.logger.Info("Queue shutdown")
			break
		}

		// func encapsultation to benefit from defer s.queue.Done()
		func(obj interface{}) {
			nodeName := obj.(string)
			defer s.queueNodeToBeUpdated.Done(obj)

			// nodePatchLimiter: client side protect to avoid flooding the api-server in case of massive update
			if s.nodePatchLimiter.TryAccept() {
				err := s.patchNodeLabels(nodeName)
				if err != nil {
					if apierrors.IsNotFound(err) {
						return // the node was deleted, no more need for update.
					}
					requeueCount := s.queueNodeToBeUpdated.NumRequeues(nodeName)
					s.logger.Error("Failed to update label", zap.String("node", nodeName), zap.Int("retry", requeueCount), zap.Error(err))
					if requeueCount > 10 {
						s.queueNodeToBeUpdated.Forget(nodeName)
						return
					}
					s.queueNodeToBeUpdated.AddRateLimited(obj) // retry with exp backoff
					MeasureNodeLabelPatchFailed.M(1)
					return
				}
				// the item was correctly processed
				s.queueNodeToBeUpdated.Forget(nodeName)
				return
			}
			s.queueNodeToBeUpdated.AddRateLimited(obj) // retry with exp backoff
			MeasureNodeLabelPatchRateLimited.M(1)
		}(obj)
	}
}

func (s *DrainoConfigurationObserverImpl) patchNodeLabels(nodeName string) error {
	s.logger.Info("Update node labels", zap.String("node", nodeName))
	node, err := s.runtimeObjectStore.Nodes().Get(nodeName)
	if err != nil {
		return err
	}
	desiredValue, outOfDate, err := s.getLabelUpdate(node)
	if err != nil {
		return err
	}

	if outOfDate {
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		err := PatchNodeLabelKey(s.globalConfig.Context, s.kclient, nodeName, ConfigurationLabelKey, desiredValue)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// Reset: remove all previous persisted values in node annotations.
// This can be useful if ever the name of the draino configuration changes
func (s *DrainoConfigurationObserverImpl) Reset() {
	if err := wait.PollImmediateInfinite(2*time.Second, func() (done bool, err error) {
		synced := s.runtimeObjectStore.Nodes().HasSynced()
		s.logger.Info("Wait for node informer to sync", zap.Bool("synced", synced))
		return synced, nil
	}); err != nil {
		s.logger.Error("Failed to sync node informer before reset labels. Reset labels cancelled.")
	}

	s.logger.Info("Resetting labels for configuration names")
	// Reset the annotations that are set by the observer
	for _, node := range s.runtimeObjectStore.Nodes().ListNodes() {
		s.logger.Info("Resetting labels for node", zap.String("node", node.Name))
		if node.Labels[ConfigurationLabelKey] != "" {
			if err := RetryWithTimeout(func() error {
				time.Sleep(2 * time.Second)
				err := PatchDeleteNodeLabelKey(s.globalConfig.Context, s.kclient, node.Name, ConfigurationLabelKey)
				if err != nil {
					s.logger.Info("Failed attempt to reset labels", zap.String("node", node.Name),
						zap.Error(err))
				}
				return err
			}, 500*time.Millisecond, 10*time.Second); err != nil {
				s.logger.Error("Failed to reset labels", zap.String("node", node.Name),
					zap.Error(err))
				continue
			}
			s.logger.Info("Labels reset done", zap.String("node", node.Name))
		}
	}
	s.logger.Info("Nodes labels reset completed")
}

func (s *DrainoConfigurationObserverImpl) HasPodWithPVCManagementEnabled(node *v1.Node) bool {
	if node == nil {
		return false
	}
	pods, err := s.runtimeObjectStore.Pods().ListPodsForNode(node.Name)
	if err != nil {
		s.logger.Error("Failed to list pod for node in DrainoConfigurationObserverImpl.HasPodWithPVCManagementEnabled", zap.String("node", node.Name), zap.Error(err))
		return false
	}
	for _, p := range pods {
		if PVCStorageClassCleanupEnabled(p, s.runtimeObjectStore, s.globalConfig.PVCManagementEnableIfNoEvictionUrl) {
			return true
		}
	}
	return false
}

func PVCStorageClassCleanupEnabled(p *v1.Pod, store RuntimeObjectStore, defaultTrueIfNoEvictionUrl bool) bool {
	valAnnotation, _ := GetAnnotationFromPodOrController(PVCStorageClassCleanupAnnotationKey, p, store)
	if valAnnotation == PVCStorageClassCleanupAnnotationTrueValue {
		return true
	}
	if valAnnotation == PVCStorageClassCleanupAnnotationFalseValue {
		return false
	}

	if defaultTrueIfNoEvictionUrl {
		_, evictionUrlFound := GetAnnotationFromPodOrController(EvictionAPIURLAnnotationKey, p, store)
		return !evictionUrlFound
	}

	return false
}

func (s *DrainoConfigurationObserverImpl) HasEvictionUrlViaAnnotation(node *v1.Node) bool {
	if node == nil {
		return false
	}
	pods, err := s.runtimeObjectStore.Pods().ListPodsForNode(node.Name)
	if err != nil {
		s.logger.Error("Failed to list pod for node in DrainoConfigurationObserverImpl.HasEvictionUrlViaAnnotation", zap.String("node", node.Name), zap.Error(err))
		return false
	}
	for _, p := range pods {
		if _, ok := GetAnnotationFromPodOrController(EvictionAPIURLAnnotationKey, p, s.runtimeObjectStore); ok {
			return true
		}
	}
	return false
}

func (s *DrainoConfigurationObserverImpl) HasPodWithUserOptOutAnnotation(node *v1.Node) bool {
	return s.hasPodThatMatchFilter(node, s.userOptOutPodFilter)
}

func (s *DrainoConfigurationObserverImpl) HasPodWithUserOptInAnnotation(node *v1.Node) bool {
	return s.hasPodThatMatchFilter(node, s.userOptInPodFilter)
}

func (s *DrainoConfigurationObserverImpl) hasPodThatMatchFilter(node *v1.Node, filter PodFilterFunc) bool {
	if node == nil {
		return false
	}
	pods, err := s.runtimeObjectStore.Pods().ListPodsForNode(node.Name)
	if err != nil {
		s.logger.Error("Failed to list pods for node in DrainoConfigurationObserverImpl.hasPodThatMatchFilter", zap.String("node", node.Name), zap.Error(err))
		return false
	}
	for _, p := range pods {
		opt, _, err := filter(*p)
		if err != nil {
			s.logger.Error("Failed to check if pod is filtered", zap.String("node", node.Name), zap.String("pod", p.Name), zap.Error(err))
			continue
		}
		if opt {
			return true
		}
	}
	return false
}

func getDrainStatusStr(node *v1.Node) string {
	drainStatus, err := GetDrainConditionStatus(node)
	if err != nil {
		return "Error"
	}
	switch {
	case drainStatus.Completed:
		return CompletedStr
	case drainStatus.Failed:
		return FailedStr
	case drainStatus.Marked:
		return ScheduledStr
	}
	return "None"
}
