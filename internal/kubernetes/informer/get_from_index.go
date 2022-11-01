package informer

import (
	"context"
	"fmt"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	cachek "k8s.io/client-go/tools/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
)

type GetSharedIndexInformer interface {
	GetSharedIndexInformer(ctx context.Context, obj clientcr.Object) (cachek.SharedIndexInformer, error)
}

func GetFromIndex[T clientcr.Object](ctx context.Context, i GetSharedIndexInformer, idx, ns, val string) ([]T, error) {
	var obj T
	indexInformer, err := i.GetSharedIndexInformer(context.Background(), obj)
	if err != nil {
		return nil, err
	}
	indexer := indexInformer.GetIndexer()
	objs, err := indexer.ByIndex(fmt.Sprintf("field:%s", idx), fmt.Sprintf("%s/%s", ns, val))
	if err != nil {
		return nil, err
	}
	return utils.ParseObjects[T](objs)
}
