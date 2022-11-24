package drain

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// as the patch path is using slashes, we have to escape the slashes in our label with '~1'
var RetryCountPatchPath = fmt.Sprintf("/metadata/annotations/%s", strings.ReplaceAll(RetryWallCountAnnotation, "/", "~1"))

type JSONPatchValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

type RetryCountPatch struct{}

var _ client.Patch = &RetryCountPatch{}

func (p *RetryCountPatch) Type() types.PatchType {
	return types.JSONPatchType
}

func (p *RetryCountPatch) Data(obj client.Object) ([]byte, error) {
	val, ok := obj.GetAnnotations()[RetryWallCountAnnotation]
	if !ok {
		return []byte{}, fmt.Errorf("object does not have retry count annotation attached")
	}

	jsonPatch := JSONPatchValue{
		Op:    "replace",
		Path:  RetryCountPatchPath,
		Value: val,
	}

	return json.Marshal([]JSONPatchValue{jsonPatch})
}
