package drain

import (
	"encoding/json"
	"fmt"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	PatchOpAdd     = "add"
	PatchOpReplace = "replace"
	PatchOpRemove  = "remove"
)

type JSONPatchValue[T any] struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value T      `json:"value"`
}

type NodeConditionPatch struct {
	ConditionType corev1.NodeConditionType
	// Operator add, replace or remove
	Operator string
}

var _ client.Patch = &NodeConditionPatch{}

func (_ *NodeConditionPatch) Type() types.PatchType {
	return types.JSONPatchType
}

func (self *NodeConditionPatch) Data(obj client.Object) ([]byte, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("cannot parse object into node")
	}

	position, condition, found := utils.FindNodeCondition(self.ConditionType, node)
	if !found {
		return nil, fmt.Errorf("cannot find condition on node")
	}

	patch := JSONPatchValue[corev1.NodeCondition]{
		Op:    self.Operator,
		Path:  fmt.Sprintf("/status/conditions/%d", position),
		Value: condition,
	}

	return json.Marshal([]JSONPatchValue[corev1.NodeCondition]{patch})
}
