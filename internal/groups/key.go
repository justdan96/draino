package groups

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	v1 "k8s.io/api/core/v1"
	"strings"
)

type GroupKey string

type GroupKeyGetter interface {
	GetGroupKey(node *v1.Node) GroupKey
	ValidateGroupKey(node *v1.Node) (valid bool, reason string)
}

type GroupKeyFromMetadata struct {
	labelsKeys                 []string
	annotationKeys             []string
	groupOverrideAnnotationKey string
	podIndexer                 index.PodIndexer
	store                      kubernetes.RuntimeObjectStore
	eventRecorder              kubernetes.EventRecorder
	logger                     logr.Logger
}

var _ GroupKeyGetter = &GroupKeyFromMetadata{}

func NewGroupKeyFromNodeMetadata(logger logr.Logger, eventRecorder kubernetes.EventRecorder, podIndexer index.PodIndexer, store kubernetes.RuntimeObjectStore, labelsKeys, annotationKeys []string, groupOverrideAnnotationKey string) GroupKeyGetter {
	return &GroupKeyFromMetadata{
		labelsKeys:                 labelsKeys,
		annotationKeys:             annotationKeys,
		groupOverrideAnnotationKey: groupOverrideAnnotationKey,
		podIndexer:                 podIndexer,
		store:                      store,
		eventRecorder:              eventRecorder,
		logger:                     logger.WithName("GroupKeyGetter"),
	}
}

func getValueOrEmpty(m map[string]string, keys []string) (values []string) {
	mInitialized := m
	if mInitialized == nil {
		mInitialized = map[string]string{}
	}
	for _, key := range keys {
		values = append(values, mInitialized[key])
	}
	return
}

const (
	GroupKeySeparator = "#"
)

func (g *GroupKeyFromMetadata) ValidateGroupKey(node *v1.Node) (valid bool, reason string) {
	if g.groupOverrideAnnotationKey != "" && node.Annotations != nil {
		if override, ok := node.Annotations[g.groupOverrideAnnotationKey]; ok && override == "" {
			return false, "Empty value for " + g.groupOverrideAnnotationKey + " annotation, group override feature will be ignored"
		}
	}
	return true, ""
}

func (g *GroupKeyFromMetadata) GetGroupKey(node *v1.Node) GroupKey {
	// slice that contains the values that will compose the groupKey
	var values []string

	// let's tackle the simple case where the user completely override the groupkey
	// node override takes over pods override. In other words if node override exists any pods value would be ignored
	if g.groupOverrideAnnotationKey != "" && node.Annotations != nil {
		if override, ok := node.Annotations[g.groupOverrideAnnotationKey]; ok && override != "" {
			// in that case we completely replace the groups, we remove the default groups.
			// for example, this allows users to define a kubernetes-cluster wide groups if the default is set to namespace
			values = strings.Split(override, ",")
			return GroupKey(strings.Join(values, GroupKeySeparator))
		}
		// if the override value is not set, we fallback to the default case with no override
	}
	// let's build the groups values from labels and annotations
	values = append(getValueOrEmpty(node.Labels, g.labelsKeys), getValueOrEmpty(node.Annotations, g.annotationKeys)...)

	if podOverride, hasPodOverride := g.getGroupKeyFromPods(node); hasPodOverride {
		values = []string{string(podOverride)}
	}
	return GroupKey(strings.Join(values, GroupKeySeparator))
}

func (g *GroupKeyFromMetadata) getGroupKeyFromPods(node *v1.Node) (GroupKey, bool) {
	if g.podIndexer == nil || g.store == nil {
		g.logger.Error(fmt.Errorf("no podIndexer or Store defined for the GroupKeyFromMetadata"), "Skipping getGroupKeyFromPods")
		// While it should not happen at runtime, this is important for testing. There are some tests where we cannot mix indexer and client from kubernetes.ClienSet and ControllerRuntime.Client
		return "", false
	}
	// check if we have any pod override
	pods, err := g.podIndexer.GetPodsByNode(context.Background(), node.Name)
	if err != nil {
		// in case of error we ignore pod nodeValues.
		// they might be taken into account next time the node is presented
		g.logger.Error(err, "failed to list pod for node", "node", node.Name)
		return "", false
	}

	var uniquePodOverride string
	var firstPodId string
	for _, p := range pods {
		podId := p.Namespace + "/" + p.Name
		if podOverride, found := kubernetes.GetAnnotationFromPodOrController(g.groupOverrideAnnotationKey, p, g.store); found {
			if uniquePodOverride == "" {
				uniquePodOverride = podOverride
				firstPodId = podId
			}
			if podOverride != uniquePodOverride {
				// With have multiple pod overrides. This is not supported. Pod overrides should be unique, user should check pod anti-affinity to ensure that this constraint is respected
				g.eventRecorder.NodeEventf(context.Background(), node, v1.EventTypeWarning, eventGroupOverrideMisconfiguration, "multiple pod overrides: "+firstPodId+" and "+podId)
				return "", false
			}
		}
	}

	if uniquePodOverride == "" {
		return "", false
	}

	// We have a pod override
	podValues := strings.Split(uniquePodOverride, ",")
	return GroupKey(strings.Join(podValues, GroupKeySeparator)), true
}
