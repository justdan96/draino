package kubernetes

import (
	"context"
	"reflect"

	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type MetaSerachResultOrigin string

const (
	MetaSerachResultOriginNode MetaSerachResultOrigin = "node"
	MetaSerachResultOriginPod  MetaSerachResultOrigin = "pod"
	MetaSerachResultOriginCtrl MetaSerachResultOrigin = "controller"
)

type MetaSerachResult[T comparable] struct {
	FoundValue bool
	Items      []MetaSerachResultItem[T]
}
type MetaSerachResultItem[T comparable] struct {
	Origin MetaSerachResultOrigin
	Value  *T
	Err    error
	Item   metav1.Object
}

type MetadataGetterFunc func(object metav1.Object) map[string]string

func GetLabels(object metav1.Object) map[string]string      { return object.GetLabels() }
func GetAnnotations(object metav1.Object) map[string]string { return object.GetAnnotations() }

func Search[T comparable](ctx context.Context, podIndexer index.PodIndexer, store RuntimeObjectStore, converter func(string) (T, error), obj client.Object, key string, stopIfFoundOnNode, stopIfFoundOnPod bool, metadataFunc MetadataGetterFunc) MetaSerachResult[T] {
	result := MetaSerachResult[T]{
		FoundValue: false,
		Items:      searchRec(ctx, podIndexer, store, converter, obj, key, stopIfFoundOnNode, stopIfFoundOnPod, metadataFunc),
	}

	for _, item := range result.Items {
		if item.Value != nil {
			result.FoundValue = true
			break
		}
	}

	return result
}

func getOriginFromObj(obj metav1.Object) MetaSerachResultOrigin {
	// TODO is this a good way?
	return MetaSerachResultOrigin(reflect.TypeOf(obj).String())
}

func searchRec[T comparable](ctx context.Context, podIndexer index.PodIndexer, store RuntimeObjectStore, converter func(string) (T, error), obj metav1.Object, key string, stopIfFoundOnNode, stopIfFoundOnPod bool, metadataFunc MetadataGetterFunc) []MetaSerachResultItem[T] {
	items := []MetaSerachResultItem[T]{}

	if val, exist := searchGeneric(obj, key, metadataFunc, converter); exist {
		items = append(items, val)
	}

	switch obj.(type) {
	case *core.Node:
		if stopIfFoundOnNode && len(items) > 0 {
			return items
		}
		pods, err := podIndexer.GetPodsByNode(ctx, obj.GetName())
		if err != nil {
			return items
		}
		for _, p := range pods {
			items = append(items, searchRec(ctx, podIndexer, store, converter, p, key, stopIfFoundOnNode, stopIfFoundOnPod, metadataFunc)...)
		}
	case *core.Pod:
		if stopIfFoundOnPod && len(items) > 0 {
			return items
		}
		if ctrl, found := GetControllerForPod(obj.(*core.Pod), store); found {
			items = append(items, searchRec(ctx, podIndexer, store, converter, ctrl, key, stopIfFoundOnNode, stopIfFoundOnPod, metadataFunc)...)
		}
	}

	return items
}

func searchGeneric[T comparable](obj metav1.Object, key string, getter MetadataGetterFunc, converter func(string) (T, error)) (MetaSerachResultItem[T], bool) {
	value, exist := getter(obj)[key]
	if !exist {
		return MetaSerachResultItem[T]{}, false
	}

	res := MetaSerachResultItem[T]{
		Origin: getOriginFromObj(obj),
		Value:  nil,
		Err:    nil,
		Item:   obj,
	}

	val, err := converter(value)
	if err == nil {
		res.Value = &val
	} else {
		res.Err = err
	}

	return res, true
}

func (a *MetaSerachResult[T]) Values() (out []T) {
	for _, item := range a.Items {
		if item.Value != nil {
			out = append(out, *item.Value)
		}
	}
	return
}

func (a *MetaSerachResult[T]) ValuesWithoutDupe() (out []T) {
	result := map[T]T{}
	for _, item := range a.Items {
		if item.Value != nil {
			result[*item.Value] = *item.Value
		}
	}
	return utils.MapValues(result)
}

func (a *MetaSerachResult[T]) HandlerError(nodeErrFunc func(*core.Node, error), podErrFunc func(*core.Pod, error)) {
	for _, item := range a.Items {
		switch item.Item.(type) {
		case *core.Node:
			nodeErrFunc(item.Item.(*core.Node), item.Err)
		case *core.Pod:
			podErrFunc(item.Item.(*core.Pod), item.Err)
		}
	}
}

func SearchAnnotationFromNodeAndThenPodOrController[T comparable](ctx context.Context, podIndexer index.PodIndexer, store RuntimeObjectStore, converter func(string) (T, error), annotationKey string, node *core.Node, stopIfFoundOnNode, stopIfFoundOnPod bool) MetaSerachResult[T] {
	return Search(ctx, podIndexer, store, converter, node, annotationKey, stopIfFoundOnNode, stopIfFoundOnPod, GetAnnotations)
}
