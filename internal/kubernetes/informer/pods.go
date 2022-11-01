package informer

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const PodsByNodeNameIdx = "pods:by:node"

type PodInformer interface {
	GetPodsByNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error)
}

func (i *Informer) GetPodsByNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error) {
	return GetFromIndex[*corev1.Pod](ctx, i, PodsByNodeNameIdx, AllNamespacesNS, nodeName)
}

func initPodIndexer(cache cachecr.Cache) error {
	return cache.IndexField(context.Background(), &corev1.Pod{}, PodsByNodeNameIdx, indexPodsByNodeName)
}

func indexPodsByNodeName(o client.Object) []string {
	pod, ok := o.(*corev1.Pod)
	if !ok {
		return []string{}
	}
	return []string{pod.Spec.NodeName}
}
