package kubernetes

import (
	"context"
	"fmt"
	xxhash_v2 "github.com/cespare/xxhash/v2"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"strconv"
)

const (
	nodeUidKey = "node-lifecycle.datadoghq.com/node-uid"
	podUidKey  = "node-lifecycle.datadoghq.com/pod-uid"
)

// This interface centralizes all k8s event interaction for this project.
// See also https://datadoghq.atlassian.net/wiki/spaces/~960205474/pages/2251949026/Draino+and+Node+Problem+Detector+Event+Inventory
// which is a datadog specific catalog of all events emit by NLA serving as documentation for users. Changes to events in this project should be reflected in that page.
type EventRecorder interface {
	NodeEventf(ctx context.Context, obj *core.Node, eventtype, reason, messageFmt string, args ...interface{})
	PodEventf(ctx context.Context, pod *core.Pod, node *core.Node, eventtype, reason, messageFmt string, args ...interface{})
	PersistentVolumeEventf(ctx context.Context, obj *core.PersistentVolume, eventtype, reason, messageFmt string, args ...interface{})
	PersistentVolumeClaimEventf(ctx context.Context, obj *core.PersistentVolumeClaim, eventtype, reason, messageFmt string, args ...interface{})
}

type eventRecorder struct {
	eventRecorder record.EventRecorder
}

// NewEventRecorder returns a new record.EventRecorder for the given client.
func NewEventRecorder(k8sEventRecorder record.EventRecorder) EventRecorder {
	return &eventRecorder{
		eventRecorder: k8sEventRecorder,
	}
}

func createSpan(ctx context.Context, operationName string, name string, eventType, reason, messageFmt string, args ...interface{}) (tracer.Span, context.Context) {
	span, ctx := tracer.StartSpanFromContext(ctx, operationName)

	span.SetTag("name", name)
	span.SetTag("eventType", eventType)
	span.SetTag("reason", reason)
	span.SetTag("message", fmt.Sprintf(messageFmt, args...))
	return span, ctx
}

func (e *eventRecorder) NodeEventf(ctx context.Context, obj *core.Node, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "NodeEvent", obj.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	// Events must be associated with this object reference, rather than the
	// node itself, in order to appear under `kubectl describe node` due to the
	// way that command is implemented.
	// https://github.com/kubernetes/kubernetes/blob/17740a2/pkg/printers/internalversion/describe.go#L2711
	nodeReference := &core.ObjectReference{Kind: "Node", Name: obj.GetName(), UID: types.UID(obj.GetName())}
	e.eventRecorder.AnnotatedEventf(nodeReference, map[string]string{
		nodeUidKey: string(obj.UID),
	}, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PodEventf(ctx context.Context, pod *core.Pod, node *core.Node, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "PodEvent", pod.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	annotations := map[string]string{
		podUidKey: string(pod.UID),
	}
	if node != nil {
		annotations[nodeUidKey] = string(node.UID)
	}

	e.eventRecorder.AnnotatedEventf(pod, annotations, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PersistentVolumeEventf(ctx context.Context, obj *core.PersistentVolume, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "PesistentVolumeEvent", obj.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

func GetSharedSpan(obj *core.Node, parentOperation string) ddtrace.SpanContext {
	spanId := generateSpanID("nla-node-drain", parentOperation, string(obj.UID))
	// parent span that is never finished
	parent, err := tracer.Extract(tracer.TextMapCarrier{
		tracer.DefaultTraceIDHeader:  strconv.FormatUint(spanId, 10),
		tracer.DefaultParentIDHeader: strconv.FormatUint(spanId, 10),
	})
	if err != nil {
		// TODO(andy) cleanup
		fmt.Printf("[andy-test] failed to setup span parent: %v", err)
	}
	return parent
}

func CreateNodeSpan(parent ddtrace.SpanContext, operationName string, opts ...tracer.StartSpanOption) tracer.Span {
	tracerOpts := []tracer.StartSpanOption{
		tracer.ServiceName("draino"),
		tracer.ResourceName("node_drain"),
		tracer.ChildOf(parent),
	}
	tracerOpts = append(tracerOpts, opts...)
	span := tracer.StartSpan(
		operationName,
		tracerOpts...,
	)

	return span
}

func generateSpanID(prefix, operation, uid string) uint64 {
	digest := xxhash_v2.New()
	// Below methods are specified in both code and documentation to always return "len(s), nil" so we can safely ignore
	// the error value
	_, _ = digest.WriteString(prefix)
	_, _ = digest.WriteString("|")
	_, _ = digest.WriteString(operation)
	_, _ = digest.WriteString("|")
	_, _ = digest.WriteString(uid)
	return digest.Sum64()
}

func (e *eventRecorder) PersistentVolumeClaimEventf(ctx context.Context, obj *core.PersistentVolumeClaim, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "PersistentVolumeClaim", obj.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}
