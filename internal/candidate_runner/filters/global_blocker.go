package filters

import (
	"context"

	v1 "k8s.io/api/core/v1"

	"github.com/planetlabs/draino/internal/kubernetes"
)

func NewGlobalBlockerFilter(globalBlocker kubernetes.GlobalBlocker) Filter {
	return FilterFromFunctionWithReason(
		"globalBlocker",
		func(ctx context.Context, n *v1.Node) (bool, string) {
			isBlocked, reason := globalBlocker.IsBlocked()
			// return true means that the node will be kept, so we have to invert the result from the locker as it will return true when it's blocked.
			return !isBlocked, reason
		},
		true,
	)
}
