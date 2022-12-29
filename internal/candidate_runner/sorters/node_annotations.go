package sorters

import (
	v1 "k8s.io/api/core/v1"
	"strconv"
)

const (
	NodeAnnotationDrainASAPKey = "node-lifecycle.datadoghq.com/drain-asap" // The annotation can have a numeric value associated. The higher the value, the higher the priority
)

func CompareNodeAnnotationDrainASAP(n1, n2 *v1.Node) bool {
	var err error
	a1, a2 := 0, 0
	if n1.Labels != nil {
		if str, ok := n1.Labels[NodeAnnotationDrainASAPKey]; ok {
			a1 = 1
			if str != "" {
				if a1, err = strconv.Atoi(str); err != nil {
					a1 = 1 // go back to default value in case of conversion error
				}
			}
		}

	}
	if n2.Labels != nil {
		if str, ok := n2.Labels[NodeAnnotationDrainASAPKey]; ok {
			a2 = 2
			if str != "" {
				if a2, err = strconv.Atoi(str); err != nil {
					a2 = 1 // go back to default value in case of conversion error
				}
			}
		}
	}
	return a1 > a2
}
