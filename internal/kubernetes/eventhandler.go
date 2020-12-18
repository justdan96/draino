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
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

const (
	// DefaultDrainBuffer is the default minimum time between node drains.
	DefaultDrainBuffer = 10 * time.Minute

	eventReasonCordonStarting       = "CordonStarting"
	eventReasonCordonBlockedByLimit = "CordonBlockedByLimit"
	eventReasonCordonSucceeded      = "CordonSucceeded"
	eventReasonCordonFailed         = "CordonFailed"
	eventReasonCordonSkip           = "CordonSkip"

	eventReasonUncordonStarting  = "UncordonStarting"
	eventReasonUncordonSucceeded = "UncordonSucceeded"
	eventReasonUncordonFailed    = "UncordonFailed"

	eventReasonDrainScheduled        = "DrainScheduled"
	eventReasonDrainSchedulingFailed = "DrainSchedulingFailed"
	eventReasonDrainStarting         = "DrainStarting"
	eventReasonDrainSucceeded        = "DrainSucceeded"
	eventReasonDrainFailed           = "DrainFailed"

	tagResultSucceeded = "succeeded"
	tagResultFailed    = "failed"

	drainRetryAnnotationKey   = "draino/drain-retry"
	drainRetryAnnotationValue = "true"

	drainoConditionsAnnotationKey = "draino.planet.com/conditions"
)

// Opencensus measurements.
var (
	MeasureNodesCordoned       = stats.Int64("draino/nodes_cordoned", "Number of nodes cordoned.", stats.UnitDimensionless)
	MeasureNodesUncordoned     = stats.Int64("draino/nodes_uncordoned", "Number of nodes uncordoned.", stats.UnitDimensionless)
	MeasureNodesDrained        = stats.Int64("draino/nodes_drained", "Number of nodes drained.", stats.UnitDimensionless)
	MeasureNodesDrainScheduled = stats.Int64("draino/nodes_drainScheduled", "Number of nodes drain scheduled.", stats.UnitDimensionless)
	MeasureLimitedCordon       = stats.Int64("draino/cordon_limited", "Number of cordon activities that have been blocked due to limits.", stats.UnitDimensionless)

	TagNodeName, _ = tag.NewKey("node_name")
	TagResult, _   = tag.NewKey("result")
	TagReason, _   = tag.NewKey("reason")
)

// A DrainingResourceEventHandler cordons and drains any added or updated nodes.
type DrainingResourceEventHandler struct {
	logger         *zap.Logger
	cordonDrainer  CordonDrainer
	eventRecorder  record.EventRecorder
	drainScheduler DrainScheduler

	podStore     PodStore
	cordonFilter PodFilterFunc

	lastDrainScheduledFor time.Time
	buffer                time.Duration

	conditions []SuppliedCondition
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

// WithConditionsFilter configures which conditions should be handled.
func WithConditionsFilter(conditions []string) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.conditions = ParseConditions(conditions)
	}
}

// WithCordonPodFilter configures a filter that may prevent to cordon nodes
// to avoid further impossible eviction when draining.
func WithCordonPodFilter(f PodFilterFunc, podStore PodStore) DrainingResourceEventHandlerOption {
	return func(d *DrainingResourceEventHandler) {
		d.cordonFilter = f
		d.podStore = podStore
	}
}

// NewDrainingResourceEventHandler returns a new DrainingResourceEventHandler.
func NewDrainingResourceEventHandler(d CordonDrainer, e record.EventRecorder, ho ...DrainingResourceEventHandlerOption) *DrainingResourceEventHandler {
	h := &DrainingResourceEventHandler{
		logger:                zap.NewNop(),
		cordonDrainer:         d,
		eventRecorder:         e,
		lastDrainScheduledFor: time.Now(),
		buffer:                DefaultDrainBuffer,
	}
	for _, o := range ho {
		o(h)
	}
	h.drainScheduler = NewDrainSchedules(d, e, h.buffer, h.logger)
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
		h.drainScheduler.DeleteSchedule(d.Key)
		return
	}
	h.drainScheduler.DeleteSchedule(n.GetName())
}

func (h *DrainingResourceEventHandler) HandleNode(n *core.Node) {
	logger := h.logger.With(zap.String("node", n.Name))
	badConditions := h.offendingConditions(n)
	if len(badConditions) == 0 {
		if shouldUncordon(n) {
			logger.Info("node will be uncordoned and schedule deleted")
			h.drainScheduler.DeleteSchedule(n.GetName())
			h.uncordon(n)
		}
		return
	}

	// First cordon the node if it is not yet cordoned
	if !n.Spec.Unschedulable {
		// check if the node passes filters
		if !h.checkCordonFilters(n) {
			logger.Info("checkCordonFilter rejected the node")
			return
		}
		done, err := h.cordon(n, badConditions)
		if err != nil {
			logger.Error(err.Error())
			return
		}
		if !done {
			return // we have probably been rate limited
		}
	}

	// Let's ensure that a drain is scheduled
	hasSChedule, failedDrain := h.drainScheduler.HasSchedule(n.GetName())
	if !hasSChedule {
		h.scheduleDrain(n)
		return
	}

	// Is there a request to retry a failed drain activity. If yes reschedule drain
	if failedDrain && HasDrainRetryAnnotation(n) {
		h.drainScheduler.DeleteSchedule(n.GetName())
		h.scheduleDrain(n)
		return
	}
}

// checkCordonFilters return true if the filtering is ok to proceed
func (h *DrainingResourceEventHandler) checkCordonFilters(n *core.Node) bool {
	if h.cordonFilter != nil && h.podStore != nil {
		pods, err := h.podStore.ListPodsForNode(n.Name)
		if err != nil {
			h.logger.Error("cannot retrieve pods for node", zap.Error(err), zap.String("node", n.Name))
			return false
		}

		for _, pod := range pods {
			ok, err := h.cordonFilter(*pod)
			if err != nil {
				h.logger.Error("filtering issue", zap.Error(err), zap.String("node", n.Name), zap.String("pod", pod.Name), zap.String("namespace", n.Name))
				return false
			}
			if !ok {
				nr := &core.ObjectReference{Kind: "Node", Name: n.Name, UID: types.UID(n.Name)}
				h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonCordonSkip, "Pod %s/%s is not in eviction scope", pod.Namespace, pod.Name)
				h.eventRecorder.Eventf(pod, core.EventTypeWarning, eventReasonCordonSkip, "Pod is blocking cordon/drain for node %s", n.Name)
				h.logger.Info("Cordon filter triggered", zap.String("node", n.Name), zap.String("pod", pod.Name))
				return false
			}
		}
	}
	return true
}

func (h *DrainingResourceEventHandler) offendingConditions(n *core.Node) []SuppliedCondition {
	var conditions []SuppliedCondition
	for _, suppliedCondition := range h.conditions {
		for _, nodeCondition := range n.Status.Conditions {
			if suppliedCondition.Type == nodeCondition.Type &&
				suppliedCondition.Status == nodeCondition.Status &&
				time.Since(nodeCondition.LastTransitionTime.Time) >= suppliedCondition.MinimumDuration {
				conditions = append(conditions, suppliedCondition)
			}
		}
	}
	return conditions
}

func shouldUncordon(n *core.Node) bool {
	if !n.Spec.Unschedulable {
		return false
	}
	previousConditions := parseConditionsFromAnnotation(n)
	if len(previousConditions) == 0 {
		return false
	}
	for _, previousCondition := range previousConditions {
		for _, nodeCondition := range n.Status.Conditions {
			if previousCondition.Type == nodeCondition.Type &&
				previousCondition.Status != nodeCondition.Status &&
				time.Since(nodeCondition.LastTransitionTime.Time) >= previousCondition.MinimumDuration {
				return true
			}
		}
	}
	return false
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
	log := h.logger.With(zap.String("node", n.GetName()))
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}

	log.Debug("Uncordoning")
	h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonUncordonStarting, "Uncordoning node")
	if err := h.cordonDrainer.Uncordon(n, removeAnnotationMutator); err != nil {
		log.Info("Failed to uncordon", zap.Error(err))
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
	log := h.logger.With(zap.String("node", n.GetName()))
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	// Events must be associated with this object reference, rather than the
	// node itself, in order to appear under `kubectl describe node` due to the
	// way that command is implemented.
	// https://github.com/kubernetes/kubernetes/blob/17740a2/pkg/printers/internalversion/describe.go#L2711
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}

	log.Debug("Cordoning")
	h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonCordonStarting, "Cordoning node")
	if err := h.cordonDrainer.Cordon(n, conditionAnnotationMutator(badConditions)); err != nil {
		if IsLimiterError(err) {
			reason := err.Error()
			tags, _ = tag.New(context.Background(), tag.Upsert(TagReason, reason))
			stats.Record(tags, MeasureLimitedCordon.M(1))
			h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonCordonBlockedByLimit, reason)
			h.logger.Info("cordon limiter", zap.String("node", n.Name), zap.String("reason", reason))
			return false, nil
		}

		log.Info("Failed to cordon", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		stats.Record(tags, MeasureNodesCordoned.M(1))
		h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonCordonFailed, "Cordoning failed: %v", err)
		return false, err
	}
	log.Info("Cordoned")
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	stats.Record(tags, MeasureNodesCordoned.M(1))
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
	return func(n *core.Node) {}
}

// drain schedule the draining activity
func (h *DrainingResourceEventHandler) scheduleDrain(n *core.Node) {
	log := h.logger.With(zap.String("node", n.GetName()))
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}
	log.Debug("Scheduling drain")
	when, err := h.drainScheduler.Schedule(n)
	if err != nil {
		if IsAlreadyScheduledError(err) {
			return
		}
		log.Info("Failed to schedule the drain activity", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		stats.Record(tags, MeasureNodesDrainScheduled.M(1))
		h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainSchedulingFailed, "Drain scheduling failed: %v", err)
		return
	}
	log.Info("Drain scheduled ", zap.Time("after", when))
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	stats.Record(tags, MeasureNodesDrainScheduled.M(1))
	h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainScheduled, "Will drain node after %s", when.Format(time.RFC3339Nano))
}

func HasDrainRetryAnnotation(n *core.Node) bool {
	return n.GetAnnotations()[drainRetryAnnotationKey] == drainRetryAnnotationValue
}
