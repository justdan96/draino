/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

const (
	// DefaultDrainBuffer is the default minimum time between node drains.
	DefaultDrainBuffer               = 10 * time.Minute
	DefaultDurationBeforeReplacement = 1 * time.Hour

	eventReasonCordonBlockedByLimit = "CordonBlockedByLimit"
	eventReasonCordonSucceeded      = "CordonSucceeded"
	eventReasonCordonFailed         = "CordonFailed"
	eventReasonCordonSkip           = "CordonSkip"

	eventReasonUncordonStarting  = "UncordonStarting"
	eventReasonUncordonSucceeded = "UncordonSucceeded"
	eventReasonUncordonFailed    = "UncordonFailed"

	eventReasonUncordonDueToPendingPodWithLocalPV = "PodBoundToNodeViaLocalPV"

	eventReasonDrainScheduled        = "DrainScheduled"
	eventReasonDrainSchedulingFailed = "DrainSchedulingFailed"
	eventReasonDrainStarting         = "DrainStarting"
	eventReasonDrainSucceeded        = "DrainSucceeded"
	eventReasonDrainFailed           = "DrainFailed"
	eventReasonDrainConfig           = "DrainConfig"

	eventReasonNodePreprovisioning          = "NodePreprovisioning"
	eventReasonNodePreprovisioningCompleted = "NodePreprovisioningCompleted"

	tagResultSucceeded = "succeeded"
	tagResultFailed    = "failed"

	newNodeRequestReasonPreprovisioning = "preprovisioning"
	newNodeRequestReasonReplacement     = "replacement"

	drainRetryAnnotationKey          = "draino/drain-retry"
	drainRetryAnnotationValue        = "true"
	drainRetryFailedAnnotationKey    = "draino/drain-retry-failed"
	drainRetryFailedAnnotationValue  = "failed"
	drainRetryRestartAnnotationValue = "restart"

	drainoConditionsAnnotationKey = "draino.planet.com/conditions"
)

// Opencensus measurements.
var (
	MeasureNodesCordoned           = stats.Int64("draino/nodes_cordoned", "Number of nodes cordoned.", stats.UnitDimensionless)
	MeasureNodesUncordoned         = stats.Int64("draino/nodes_uncordoned", "Number of nodes uncordoned.", stats.UnitDimensionless)
	MeasureNodesDrained            = stats.Int64("draino/nodes_drained", "Number of nodes drained.", stats.UnitDimensionless)
	MeasureNodesDrainScheduled     = stats.Int64("draino/nodes_drainScheduled", "Number of nodes drain scheduled.", stats.UnitDimensionless)
	MeasureLimitedCordon           = stats.Int64("draino/cordon_limited", "Number of cordon activities that have been blocked due to limits.", stats.UnitDimensionless)
	MeasureSkippedCordon           = stats.Int64("draino/cordon_skipped", "Number of cordon activities that have been skipped due filtering.", stats.UnitDimensionless)
	MeasureNodesReplacementRequest = stats.Int64("draino/nodes_replacement_request", "Number of nodes replacement requested.", stats.UnitDimensionless)
	MeasurePreprovisioningLatency  = stats.Float64("draino/nodes_preprovisioning_latency", "Latency to get a node preprovisioned", stats.UnitMilliseconds)

	TagNodeName, _                   = tag.NewKey("node_name")
	TagConditions, _                 = tag.NewKey("conditions")
	TagTeam, _                       = tag.NewKey("team")
	TagNodegroupName, _              = tag.NewKey("nodegroup_name")
	TagNodegroupNamespace, _         = tag.NewKey("nodegroup_namespace")
	TagResult, _                     = tag.NewKey("result")
	TagReason, _                     = tag.NewKey("reason")
	TagFailureCause, _               = tag.NewKey("failure_cause")
	TagInScope, _                    = tag.NewKey("in_scope")
	TagDrainStatus, _                = tag.NewKey("drain_status")
	TagPreprovisioning, _            = tag.NewKey("preprovisioning")
	TagPVCManagement, _              = tag.NewKey("pvc_management")
	TagDrainRetry, _                 = tag.NewKey("drain_retry")
	TagDrainRetryFailed, _           = tag.NewKey("drain_retry_failed")
	TagUserOptOutViaPodAnnotation, _ = tag.NewKey("user_opt_out_via_pod_annotation")
	TagUserOptInViaPodAnnotation, _  = tag.NewKey("user_opt_in_via_pod_annotation")
	TagUserEvictionURL, _            = tag.NewKey("eviction_url")
)

// A DrainingResourceEventHandler cordons and drains any added or updated nodes.
type DrainingResourceEventHandler struct {
	logger         *zap.Logger
	kubeClient     kubernetes.Interface
	cordonDrainer  CordonDrainer
	eventRecorder  record.EventRecorder
	drainScheduler DrainScheduler

	objectsStore RuntimeObjectStore
	cordonFilter PodFilterFunc
	globalLocker GlobalBlocker

	lastDrainScheduledFor        time.Time
	buffer                       time.Duration
	schedulingBackoffDelay       time.Duration
	labelsKeyForDrainGroups      []string
	preprovisioningConfiguration NodePreprovisioningConfiguration

	conditions []SuppliedCondition

	durationWithCompletedStatusBeforeReplacement time.Duration
}

// DrainingResourceEventHandlerOption configures an DrainingResourceEventHandler.
type DrainingResourceEventHandlerOption func(d *DrainingResourceEventHandler)

// WithLogger configures a DrainingResourceEventHandler to use the supplied
// logger.
func WithLogger(l *zap.Logger) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.logger = l
	}
}

// WithDrainBuffer configures the minimum time between scheduled drains.
func WithDrainBuffer(d time.Duration) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.buffer = d
	}
}

// WithSchedulingBackoffDelay configures the backoff delay between retry schedules.
func WithSchedulingBackoffDelay(d time.Duration) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.schedulingBackoffDelay = d
	}
}

// WithDurationWithCompletedStatusBeforeReplacement configures the time we wait with Completed drain status before asking for node replacement
func WithDurationWithCompletedStatusBeforeReplacement(d time.Duration) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.durationWithCompletedStatusBeforeReplacement = d
	}
}

// WithConditionsFilter configures which conditions should be handled.
func WithConditionsFilter(conditions []string) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.conditions = ParseConditions(conditions)
	}
}

// WithCordonPodFilter configures a filter that may prevent to cordon nodes
// to avoid further impossible eviction when draining.
func WithCordonPodFilter(f PodFilterFunc) DrainingResourceEventHandlerOption {
	return func(d *DrainingResourceEventHandler) {
		d.cordonFilter = f
	}
}

// WithGlobalBlocking configures a bool that may prevent cordon nodes due to % nodes UnReady
func WithGlobalBlocking(globalLocker GlobalBlocker) DrainingResourceEventHandlerOption {
	return func(d *DrainingResourceEventHandler) {
		d.globalLocker = globalLocker
	}
}

// WithDrainGroups configures draining groups. Schedules are done per groups
func WithDrainGroups(labelKeysForDrainGroup string) DrainingResourceEventHandlerOption {
	var drainGroup []string
	if labelKeysForDrainGroup != "" {
		drainGroup = strings.Split(labelKeysForDrainGroup, ",")
	}
	return func(d *DrainingResourceEventHandler) {
		d.labelsKeyForDrainGroups = drainGroup
	}
}

// WithPreprovisioningConfiguration configures the preprovisioning (timeout, retries)
func WithPreprovisioningConfiguration(config NodePreprovisioningConfiguration) DrainingResourceEventHandlerOption {
	return func(d *DrainingResourceEventHandler) {
		d.preprovisioningConfiguration = config
	}
}

// NewDrainingResourceEventHandler returns a new DrainingResourceEventHandler.
func NewDrainingResourceEventHandler(kubeClient kubernetes.Interface, d CordonDrainer, store RuntimeObjectStore, e record.EventRecorder, ho ...DrainingResourceEventHandlerOption) *DrainingResourceEventHandler {
	h := &DrainingResourceEventHandler{
		logger:                 zap.NewNop(),
		kubeClient:             kubeClient,
		cordonDrainer:          d,
		eventRecorder:          e,
		lastDrainScheduledFor:  time.Now(),
		buffer:                 DefaultDrainBuffer,
		schedulingBackoffDelay: DefaultSchedulingRetryBackoffDelay,
		objectsStore:           store,
	}
	for _, o := range ho {
		o(h)
	}
	h.drainScheduler = NewDrainSchedules(d, e, h.buffer, h.schedulingBackoffDelay, h.labelsKeyForDrainGroups, h.conditions, h.preprovisioningConfiguration, h.logger, h.globalLocker)
	return h
}

// OnAdd cordons and drains the added node.
func (h *DrainingResourceEventHandler) OnAdd(obj interface{}) {
	n, ok := obj.(*core.Node)
	if !ok {
		return
	}
	h.HandleNode(n)
}

// OnUpdate cordons and drains the updated node.
func (h *DrainingResourceEventHandler) OnUpdate(_, newObj interface{}) {
	h.OnAdd(newObj)
}

// OnDelete does nothing. There's no point cordoning or draining deleted nodes.
func (h *DrainingResourceEventHandler) OnDelete(obj interface{}) {
	n, ok := obj.(*core.Node)
	if !ok {
		d, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		h.drainScheduler.DeleteScheduleByName(d.Key)
		return
	}

	h.drainScheduler.DeleteSchedule(n)
}

func (h *DrainingResourceEventHandler) HandleNode(n *core.Node) {
	logger := LoggerForNode(n, h.logger)
	LogForVerboseNode(h.logger, n, "HandleNode")
	// Let proceed only if the informers have synced to avoid error logs at start up.
	if h.objectsStore != nil && !h.objectsStore.HasSynced() {
		h.logger.Warn("Waiting informer to sync")
		return
	}
	// If the node is already cordon we may need to check if it should be uncordon, in case:
	// - no more bad condition
	// - it is cordon but still hold a PV needed by a pod that is pending schedule
	// - cordon filters are not passing anymore
	if n.Spec.Unschedulable {
		uncordon, err := h.shouldUncordon(n)
		if err != nil {
			logger.Error("Can't check if the node should be uncordon")
			return
		}
		if uncordon {
			LogForVerboseNode(h.logger, n, "Deleting schedule and uncordoning")
			h.drainScheduler.DeleteSchedule(n)
			h.uncordon(n)
			logger.Info("Uncordon")
			return
		}
		LogForVerboseNode(h.logger, n, "Not uncordoning")
	}

	if HasDrainRetryFailedAnnotation(n) {
		LogForVerboseNode(h.logger, n, "Failed Retry Annotation")
		if n.Spec.Unschedulable {
			nr := &core.ObjectReference{Kind: "Node", Name: n.Name, UID: types.UID(n.Name)}
			h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonDrainFailed, "Drain still failing after multiple retries. Uncordoning and ignoring the node.")
			h.drainScheduler.DeleteSchedule(n)
			h.uncordon(n)
			logger.Info("Uncordon, Drain still failing after multiple retries.")
		}
		return
	}

	if HasDrainRetryRestartAnnotation(n) {
		LogForVerboseNode(h.logger, n, "Restart Retry Annotation")
		h.drainScheduler.DeleteSchedule(n)
		h.uncordon(n)
		// Let's go back to initial state for the retry annotation
		if err := h.cordonDrainer.ResetRetryAnnotation(n); err != nil {
			logger.Error("Failed to reset retry annotation", zap.Error(err))
		}
		return
	}

	if h.globalLocker != nil {
		if locked, reason := h.globalLocker.IsBlocked(); locked {
			logger.Info("Temporarily blocked due to lock: " + reason)
			return
		}
	}

	badConditions := GetNodeOffendingConditions(n, h.conditions)
	LogForVerboseNode(h.logger, n, fmt.Sprintf("Offending conditions count %d", len(badConditions)))
	if len(badConditions) == 0 {
		return
	}

	// First cordon the node if it is not yet cordoned
	if !n.Spec.Unschedulable {
		// Check if the node is not needed due to a local PV and a pending pod trying to land on that node
		podsWithPVCBoundToThatNode, err := GetUnscheduledPodsBoundToNodeByPV(n, h.objectsStore, logger)
		if err != nil {
			logger.Error(err.Error())
			return
		}
		LogForVerboseNode(h.logger, n, fmt.Sprintf("podsWithPVCBoundToThatNode count %d", len(podsWithPVCBoundToThatNode)))
		if len(podsWithPVCBoundToThatNode) > 0 {
			LogForVerboseNode(logger, n, "Cordon Skip: Pod"+podsWithPVCBoundToThatNode[0].ResourceVersion+" need to be scheduled on node")
			nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}
			h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonUncordonDueToPendingPodWithLocalPV, "Pod "+podsWithPVCBoundToThatNode[0].Name+" needs that node due to local PV. Not cordoning the node.")
			return
		}

		// check if the node passes filters
		if !h.checkCordonFilters(n) {
			LogForVerboseNode(h.logger, n, "Not passing cordon filters")
			return
		}
		done, err := h.cordon(n, badConditions)
		LogForVerboseNode(h.logger, n, "Cordon attempt", zap.Bool("done", done), zap.Error(err))
		if err != nil {
			logger.Error(err.Error())
			return
		}
		if !done {
			return // we have probably been rate limited
		}
	}

	pods, err := h.cordonDrainer.GetPodsToDrain(n.GetName(), h.objectsStore.Pods())
	if err != nil {
		logger.Error(err.Error())
		return
	}

	drainStatus, err := GetDrainConditionStatus(n)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	LogForVerboseNode(h.logger, n, "drainStatus",
		zap.Bool("marked", drainStatus.Marked),
		zap.Bool("completed", drainStatus.Completed),
		zap.Bool("failed", drainStatus.Failed),
		zap.Error(err))

	// Check if empty node is blocked due to minSize. If yes request a node replacement
	if len(pods) == 0 && drainStatus.Completed {
		elapseSinceCompleted := time.Since(drainStatus.LastTransition)
		if elapseSinceCompleted > h.durationWithCompletedStatusBeforeReplacement {
			// This node probably blocked due to minSize set on the nodegroup
			status, err := h.cordonDrainer.ReplaceNode(n)
			LogForVerboseNode(h.logger, n, "node replacement", zap.String("status", string(status)), zap.Error(err))
		}
		return // we are waiting for that node to be removed from the cluster by the CA
	}

	if drainStatus.Failed {
		// Is there a request to retry a failed drain activity. If yes reschedule drain
		if HasDrainRetryAnnotation(n) {
			h.drainScheduler.DeleteSchedule(n)
			if drainStatus.FailedCount >= h.cordonDrainer.GetMaxDrainAttemptsBeforeFail() {
				logger.Warn("Drain Failed: MaxDrainAttempts reached")
				// the uncordoning is done earlier in that sequence if it makes sense because we want to be before the global locker.
				return
			}
			h.scheduleDrain(n, drainStatus.FailedCount)
			LogForVerboseNode(h.logger, n, "retry with new schedule")
			return
		}
		// Else we leave it like this
		return
	}

	// The node may have been cordon by a user. Let's check if the cordon filters of draino are valid before doing any schedule
	if h.checkCordonFilters(n) {
		// Let's ensure that a drain is scheduled
		hasSchedule, failedSched := h.drainScheduler.HasSchedule(n)
		LogForVerboseNode(h.logger, n, "hasSchedule", zap.Bool("hasSchedule", hasSchedule), zap.Bool("failedSchedule", failedSched))
		if !hasSchedule {
			h.scheduleDrain(n, drainStatus.FailedCount)
			return
		}
	} else {
		logger.Info("Node is cordon but it is not passing cordon filters. Not scheduling any drain.")
	}
}

// checkCordonFilters return true if the filtering is ok to proceed
func (h *DrainingResourceEventHandler) checkCordonFilters(n *core.Node) bool {
	if h.cordonFilter != nil && h.objectsStore != nil && h.objectsStore.Pods() != nil {
		pods, err := h.objectsStore.Pods().ListPodsForNode(n.Name)
		if err != nil {
			h.logger.Error("cannot retrieve pods for node", zap.Error(err), zap.String("node", n.Name))
			return false
		}
		tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
		for _, pod := range pods {
			ok, reason, err := h.cordonFilter(*pod)
			LogForVerboseNode(h.logger, n, "Cordon Filter", zap.String("pod", pod.Name), zap.String("reason", reason), zap.Bool("ok", ok))
			if err != nil {
				tags, _ := tag.New(tags, tag.Upsert(TagReason, "error"))
				StatRecordForEachCondition(tags, n, h.conditions, MeasureSkippedCordon.M(1))
				h.logger.Error("filtering issue", zap.Error(err), zap.String("node", n.Name), zap.String("pod", pod.Name), zap.String("namespace", n.Name))
				return false
			}
			if !ok {
				nr := &core.ObjectReference{Kind: "Node", Name: n.Name, UID: types.UID(n.Name)}
				tags, _ := tag.New(tags, tag.Upsert(TagReason, reason))
				StatRecordForEachCondition(tags, n, h.conditions, MeasureSkippedCordon.M(1))
				h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonCordonSkip, "Pod %s/%s is not in eviction scope", pod.Namespace, pod.Name)
				h.eventRecorder.Eventf(pod, core.EventTypeWarning, eventReasonCordonSkip, "Pod is blocking cordon/drain for node %s", n.Name)
				h.logger.Debug("Cordon filter triggered", zap.String("node", n.Name), zap.String("pod", pod.Name))
				return false
			}
		}
	}
	return true
}

func (h *DrainingResourceEventHandler) shouldUncordon(n *core.Node) (bool, error) {
	if !n.Spec.Unschedulable {
		return false, nil
	}
	logger := LoggerForNode(n, h.logger)

	drainStatus, err := GetDrainConditionStatus(n)
	if err != nil {
		logger.Error(err.Error())
		return false, err
	}
	// Only take the nodes that have been cordon by `draino` (with schedule)
	if !drainStatus.Marked {
		return false, nil
	}

	badConditions := GetNodeOffendingConditions(n, h.conditions)
	if len(badConditions) == 0 {
		LogForVerboseNode(logger, n, "No offending condition")
		previousConditions := parseConditionsFromAnnotation(n)
		if len(previousConditions) > 0 {
			for _, previousCondition := range previousConditions {
				for _, nodeCondition := range n.Status.Conditions {
					if previousCondition.Type == nodeCondition.Type &&
						previousCondition.Status != nodeCondition.Status &&
						time.Since(nodeCondition.LastTransitionTime.Time) >= previousCondition.MinimumDuration {
						return true, nil
					}
				}
			}
		}
	}

	// Check if the node need to be uncordon because a pod is bound to it due to a PV/PVC (local volume)
	pods, err := GetUnscheduledPodsBoundToNodeByPV(n, h.objectsStore, logger)
	if err != nil {
		return false, err
	}
	if len(pods) > 0 {
		logger.Info("Pod needs to be scheduled on node", zap.String("pod", pods[0].Name))
		nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}
		h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonUncordonDueToPendingPodWithLocalPV, "Pod "+pods[0].Name+" needs that node due to local PV. Uncordoning the node.")
		return true, nil
	}

	// Check if the cordon filter are still valid for that node
	if !h.checkCordonFilters(n) {
		logger.Info("Not passing cordon filters anymore")
		return true, nil
	}

	return false, nil
}

func parseConditionsFromAnnotation(n *core.Node) []SuppliedCondition {
	if n.Annotations == nil {
		return nil
	}
	if n.Annotations[drainoConditionsAnnotationKey] == "" {
		return nil
	}
	rawConditions := strings.Split(n.Annotations[drainoConditionsAnnotationKey], ";")
	return ParseConditions(rawConditions)
}

func (h *DrainingResourceEventHandler) uncordon(n *core.Node) {
	log := LoggerForNode(n, h.logger)
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}

	log.Debug("Uncordoning")
	h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonUncordonStarting, "Uncordoning node")
	if err := h.cordonDrainer.Uncordon(n, removeAnnotationMutator); err != nil {
		log.Error("Failed to uncordon", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		stats.Record(tags, MeasureNodesUncordoned.M(1))
		h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonUncordonFailed, "Uncordoning failed: %v", err)
		return
	}
	log.Info("Uncordoned")
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	stats.Record(tags, MeasureNodesUncordoned.M(1))
	h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonUncordonSucceeded, "Uncordoned node")
}

func removeAnnotationMutator(n *core.Node) {
	delete(n.Annotations, drainoConditionsAnnotationKey)
}

func (h *DrainingResourceEventHandler) cordon(n *core.Node, badConditions []SuppliedCondition) (cordon bool, err error) {
	log := LoggerForNode(n, h.logger)
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	// Events must be associated with this object reference, rather than the
	// node itself, in order to appear under `kubectl describe node` due to the
	// way that command is implemented.
	// https://github.com/kubernetes/kubernetes/blob/17740a2/pkg/printers/internalversion/describe.go#L2711
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}

	if err := h.cordonDrainer.Cordon(n, conditionAnnotationMutator(badConditions)); err != nil {
		if IsLimiterError(err) {
			reason := err.Error()
			tags, _ = tag.New(context.Background(), tag.Upsert(TagReason, reason))
			StatRecordForEachCondition(tags, n, badConditions, MeasureLimitedCordon.M(1))
			h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonCordonBlockedByLimit, reason)
			log.Debug("cordon limiter", zap.String("node", n.Name), zap.String("reason", reason))
			return false, nil
		}

		log.Error("Failed to cordon", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		StatRecordForEachCondition(tags, n, badConditions, MeasureNodesCordoned.M(1))
		h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonCordonFailed, "Cordoning failed: %v", err)
		return false, err
	}
	log.Info("Cordoned")
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	StatRecordForEachCondition(tags, n, badConditions, MeasureNodesCordoned.M(1))
	h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonCordonSucceeded, "Cordoned node")
	return true, nil
}

func conditionAnnotationMutator(conditions []SuppliedCondition) func(*core.Node) {
	var value []string
	for _, c := range conditions {
		value = append(value, fmt.Sprintf("%v=%v,%v", c.Type, c.Status, c.MinimumDuration))
	}
	return func(n *core.Node) {
		if n.Annotations == nil {
			n.Annotations = make(map[string]string)
		}
		n.Annotations[drainoConditionsAnnotationKey] = strings.Join(value, ";")
	}
}

// drain schedule the draining activity
func (h *DrainingResourceEventHandler) scheduleDrain(n *core.Node, failedCount int32) {
	log := LoggerForNode(n, h.logger)
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}
	log.Debug("Scheduling drain")
	when, err := h.drainScheduler.Schedule(n, failedCount)
	if err != nil {
		if IsAlreadyScheduledError(err) {
			return
		}
		log.Error("Failed to schedule the drain activity", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		StatRecordForEachCondition(tags, n, GetNodeOffendingConditions(n, h.conditions), MeasureNodesDrainScheduled.M(1))
		h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainSchedulingFailed, "Drain scheduling failed: %v", err)
		return
	}
	log.Info("Drain scheduled ", zap.Time("after", when))
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	StatRecordForEachCondition(tags, n, GetNodeOffendingConditions(n, h.conditions), MeasureNodesDrainScheduled.M(1))
	h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainScheduled, "Will drain node after %s", when.Format(time.RFC3339Nano))
}

func HasDrainRetryAnnotation(n *core.Node) bool {
	return n.GetAnnotations()[drainRetryAnnotationKey] == drainRetryAnnotationValue
}

func HasDrainRetryFailedAnnotation(n *core.Node) bool {
	// the second part of the OR is needed while we are migrating this annotation to the  new key. At some point we can remove it.
	return n.GetAnnotations()[drainRetryFailedAnnotationKey] == drainRetryFailedAnnotationValue || n.GetAnnotations()[drainRetryAnnotationKey] == drainRetryFailedAnnotationValue
}

func HasDrainRetryRestartAnnotation(n *core.Node) bool {
	return n.GetAnnotations()[drainRetryFailedAnnotationKey] == drainRetryRestartAnnotationValue
}
