package k8sclient

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
)

// --------------------------------------------
// Condition Patch
// --------------------------------------------
type NodeConditionPatch struct {
	ConditionType corev1.NodeConditionType
}

var _ client.Patch = &NodeConditionPatch{}

func (_ *NodeConditionPatch) Type() types.PatchType {
	return types.StrategicMergePatchType
}

func (self *NodeConditionPatch) Data(obj client.Object) ([]byte, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("cannot parse object into node")
	}

	_, condition, found := utils.FindNodeCondition(self.ConditionType, node)
	if !found {
		return nil, fmt.Errorf("cannot find condition on node")
	}

	// This is inspired by a kubernetes function that is updating the node conditions in the same way
	// https://github.com/kubernetes/kubernetes/blob/fa1b6765d55c3f3c00299a9e279732342cfb00f7/staging/src/k8s.io/component-helpers/node/util/conditions.go#L45
	// It was tested on a local cluster and works for both create & updates.
	return json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []corev1.NodeCondition{condition},
		},
	})
}

// --------------------------------------------
// JSON Patches
// --------------------------------------------
type JSONPatchValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

type AnnotationPatch struct {
	Key   string
	Value string
}

var _ client.Patch = &AnnotationPatch{}

func (p *AnnotationPatch) Type() types.PatchType {
	return types.JSONPatchType
}
func (p *AnnotationPatch) Data(obj client.Object) ([]byte, error) {
	jsonPatch := JSONPatchValue{
		Op: "replace",
		// as the patch path might include "/", we have to escape the slashes in the annotation path with '~1'
		Path:  fmt.Sprintf("/metadata/annotations/%s", strings.ReplaceAll(p.Key, "/", "~1")),
		Value: p.Value,
	}

	return json.Marshal([]JSONPatchValue{jsonPatch})
}
