package filters

import (
	v1 "k8s.io/api/core/v1"
)

func NewNodeTerminatingFilter(group string) Filter {
	return FilterFromFunction("node_terminating", group,
		func(n *v1.Node) bool {
			return n.DeletionTimestamp == nil || n.DeletionTimestamp.IsZero()
		})
}
