package groups

import (
	v1 "k8s.io/api/core/v1"
	"strings"
)

type GroupKey string

type GroupKeyGetter interface {
	GetGroupKey(node *v1.Node) GroupKey
}

type GroupKeyFromMetadata struct {
	labelsKeys                 []string
	annotationKeys             []string
	groupOverrideAnnotationKey string
}

func NewGroupKeyFromNodeMetadata(labelsKeys, annotationKeys []string, groupOverrideAnnotationKey string) GroupKeyGetter {
	return &GroupKeyFromMetadata{
		labelsKeys:                 labelsKeys,
		annotationKeys:             annotationKeys,
		groupOverrideAnnotationKey: groupOverrideAnnotationKey,
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

func (g *GroupKeyFromMetadata) GetGroupKey(node *v1.Node) GroupKey {
	// slice that contains the values that will compose the groupKey
	var values []string

	// let's tackle the simple case where the user completely override the groupkey
	if g.groupOverrideAnnotationKey != "" && node.Annotations != nil {
		if override, ok := node.Annotations[g.groupOverrideAnnotationKey]; ok && override != "" {
			// in that case we completely replace the groups, we remove the default groups.
			// for example, this allows users to define a kubernetes-cluster wide groups if the default is set to namespace
			values = strings.Split(override, ",")
		}
	} else { // let's build the groups values from labels and annotations
		values = append(getValueOrEmpty(node.Labels, g.labelsKeys), getValueOrEmpty(node.Annotations, g.annotationKeys)...)
	}
	return GroupKey(strings.Join(values, GroupKeySeparator))
}
