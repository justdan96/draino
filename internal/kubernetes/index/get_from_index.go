package index

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	cachek "k8s.io/client-go/tools/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
)

type GetSharedIndexInformer interface {
	GetSharedIndexInformer(ctx context.Context, obj clientcr.Object) (cachek.SharedIndexInformer, error)
}

func GetFromIndex[T clientcr.Object](ctx context.Context, i GetSharedIndexInformer, idx, val string, obj T) ([]T, error) {
	indexInformer, err := i.GetSharedIndexInformer(ctx, obj)
	if err != nil {
		return nil, err
	}
	indexer := indexInformer.GetIndexer()
	objs, err := indexer.ByIndex(idx, val)
	if err != nil {
		return nil, err
	}
	return utils.ParseObjects[T](objs)
}
