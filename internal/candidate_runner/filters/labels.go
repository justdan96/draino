package filters

import (
	"github.com/planetlabs/draino/internal/kubernetes"
)

func NewNodeWithLabelFilter(group string, nodeLabelsFilterFunc kubernetes.NodeLabelFilterFunc) Filter {
	return FilterFromFunction("labels", group, NodeFilterFuncFromInterfaceFunc(nodeLabelsFilterFunc))
}
