package filters

import (
	"context"

	v1 "k8s.io/api/core/v1"

	"github.com/planetlabs/draino/internal/kubernetes"
)

func NewNodeWithConditionFilter(conditions []kubernetes.SuppliedCondition) Filter {
	return FilterFromFunctionWithReason(
		"conditions",
		func(ctx context.Context, n *v1.Node) (bool, string) {
			badConditions := kubernetes.GetNodeOffendingConditions(n, conditions)
			if len(badConditions) == 0 {
				return false, "no_condition"
			}
			badConditionsStr := kubernetes.GetConditionsTypes(badConditions)
			if !kubernetes.AtLeastOneConditionAcceptedByTheNode(badConditionsStr, n) {
				return false, "no_allowed_condition"
			}
			return true, ""
		},
		true,
	)

}
