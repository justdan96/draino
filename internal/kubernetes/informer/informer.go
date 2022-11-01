package informer

import (
	"context"
	"fmt"

	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const AllNamespacesNS = "__all_namespaces"

var (
	_ PDBInformer = &Informer{}
	_ PodInformer = &Informer{}

	_ GetSharedIndexInformer = &Informer{}
)

type Informer struct {
	client clientcr.Client
	cache  cachecr.Cache
}

func New(mgr manager.Manager) (*Informer, error) {
	informer := &Informer{
		client: mgr.GetClient(),
		cache:  mgr.GetCache(),
	}

	if err := informer.Init(); err != nil {
		return nil, err
	}

	return informer, nil
}

func (i *Informer) Init() error {
	if err := initPDBIndexer(i.client, i.cache); err != nil {
		return err
	}
	if err := initPodIndexer(i.cache); err != nil {
		return err
	}
	return nil
}

func (i *Informer) WaitForCacheSync(ctx context.Context) bool {
	return i.cache.WaitForCacheSync(ctx)
}

func (i *Informer) GetSharedIndexInformer(ctx context.Context, obj clientcr.Object) (cachek.SharedIndexInformer, error) {
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
