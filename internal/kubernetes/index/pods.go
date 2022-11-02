package index

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
)

const PodsByNodeNameIdx = "pods:by:node"

type PodIndexer interface {
	GetPodsByNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error)
}

func (i *Indexer) GetPodsByNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error) {
	return GetFromIndex(ctx, i, PodsByNodeNameIdx, nodeName, &corev1.Pod{})
}

func initPodIndexer(cache cachecr.Cache) error {
	informer, err := cache.GetInformer(context.Background(), &corev1.Pod{})
	if err != nil {
		return err
	}
	return informer.AddIndexers(map[string]cachek.IndexFunc{
		PodsByNodeNameIdx: indexPodsByNodeName,
	})
}

func indexPodsByNodeName(o interface{}) ([]string, error) {
	pod, ok := o.(*corev1.Pod)
	if !ok {
		return nil, errors.New("cannot parse pod object for indexing")
	}
	return []string{pod.Spec.NodeName}, nil
}
