package informer

import (
	"context"
	"fmt"

	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
)

// Make sure that the Informer is implementing all the required interfaces
var (
	_ PDBIndexer = &Indexer{}
	_ PodIndexer = &Indexer{}

	_ GetSharedIndexInformer = &Indexer{}
)

type Indexer struct {
	client clientcr.Client
	cache  cachecr.Cache
}

func New(client clientcr.Client, cache cachecr.Cache) (*Indexer, error) {
	informer := &Indexer{client, cache}

	if err := informer.Init(); err != nil {
		return nil, err
	}

	return informer, nil
}

func (i *Indexer) Init() error {
	if err := initPDBIndexer(i.client, i.cache); err != nil {
		return err
	}
	if err := initPodIndexer(i.cache); err != nil {
		return err
	}
	return nil
}

func (i *Indexer) WaitForCacheSync(ctx context.Context) bool {
	return i.cache.WaitForCacheSync(ctx)
}

func (i *Indexer) GetSharedIndexInformer(ctx context.Context, obj clientcr.Object) (cachek.SharedIndexInformer, error) {
	informer, err := i.cache.GetInformer(ctx, obj)
	if err != nil {
		return nil, err
	}

	indexInformer, ok := informer.(cachek.SharedIndexInformer)
	if !ok {
		return nil, fmt.Errorf("unable to create shared index informer")
	}

	return indexInformer, nil
}
