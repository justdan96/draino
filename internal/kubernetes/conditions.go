package kubernetes

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	core "k8s.io/api/core/v1"
)

const DefaultExpectedResolutionTime = time.Hour * 24 * 7

// SuppliedCondition defines the condition will be watched.
type SuppliedCondition struct {
	Type   core.NodeConditionType
	Status core.ConditionStatus
	// Draino starts acting on a node with this condition after MinimumDuration has elapsed.
	// If a node has multiple conditions, the smallest MinimumDuration is applied. Default is 0.
	MinimumDuration time.Duration
	// ExpectedResolutionTime is the duration given to draino and cluster-autoscaler (for
	// in-scope nodes) or users (for out-of-scope nodes) to drain and scale down
	// nodes with this condition. ExpectedResolutionTime and MinimumDuration start concurrently, so
	// ExpectedResolutionTime should be higher than MinimumDuration. After ExpectedResolutionTime, the nodes need
	// attention (metric->monitor->SLO). A higher priority is typically associated
	// with a lower time limit. Default is 7 days for now.
	ExpectedResolutionTime time.Duration
}

func GetNodeOffendingConditions(n *core.Node, suppliedConditions []SuppliedCondition) []SuppliedCondition {
	var conditions []SuppliedCondition
	for _, suppliedCondition := range suppliedConditions {
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

func IsOverdue(n *core.Node, suppliedCondition SuppliedCondition) bool {
	for _, nodeCondition := range n.Status.Conditions {
		if suppliedCondition.Type == nodeCondition.Type &&
			suppliedCondition.Status == nodeCondition.Status &&
			time.Since(nodeCondition.LastTransitionTime.Time) >= suppliedCondition.ExpectedResolutionTime {
			return true
		}
	}
	return false
}

func GetConditionsTypes(conditions []SuppliedCondition) []string {
	result := make([]string, len(conditions))
	for i := range conditions {
		result[i] = string(conditions[i].Type)
	}
	return result
}

func StatRecordForEachCondition(ctx context.Context, node *core.Node, conditions []SuppliedCondition, m stats.Measurement) {
	tagsWithNg, _ := nodeTags(ctx, node)
	for _, c := range GetConditionsTypes(conditions) {
		tags, _ := tag.New(tagsWithNg, tag.Upsert(TagConditions, c))
		stats.Record(tags, m)
	}
}
