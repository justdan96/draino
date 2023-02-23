package filters

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
)

func NewNodeWithConditionFilter(conditions []kubernetes.SuppliedCondition) Filter {
	return FilterFromFunctionWithReason(
		"conditions",
		func(ctx context.Context, n *v1.Node) (bool, string) {
			badConditions := kubernetes.GetNodeOffendingConditions(n, conditions)
			if len(badConditions) == 0 {
				return false, "no_condition"
			}
			// This will make sure that the drain_runner is ignoring nodes with unrecoverable conditions
			// as they will be handled by a different routine.
			if kubernetes.AtLeastOneForceEvictCondition(badConditions) {
				return false, "found_unrecoverable_condition"
			}
			badConditionsStr := kubernetes.GetConditionsTypes(badConditions)
			if !kubernetes.AtLeastOneConditionAcceptedByTheNode(badConditionsStr, n) {
				return false, "no_allowed_condition"
			}
			return true, ""
		},
	)

}
