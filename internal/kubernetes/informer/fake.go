package informer

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	fakecache "sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func createCache(inf informers.SharedInformerFactory, scheme *runtime.Scheme) cachecr.Cache {
	return &fakecache.FakeInformers{
		InformersByGVK: map[schema.GroupVersionKind]cache.SharedIndexInformer{
			policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"): inf.Policy().V1().PodDisruptionBudgets().Informer(),
			corev1.SchemeGroupVersion.WithKind("Pod"):                   inf.Core().V1().Pods().Informer(),
		},
		Scheme: scheme,
	}

}

func NewFakeInformer(ch chan struct{}, objects []runtime.Object) (*Informer, error) {
	fakeClient := fake.NewFakeClient(objects...)
	fakeKubeClient := fakeclient.NewSimpleClientset(objects...)

	inf := informers.NewSharedInformerFactory(fakeKubeClient, 10*time.Second)
	cache := createCache(inf, fakeClient.Scheme())

	informer, err := New(fakeClient, cache)
	if err != nil {
		return nil, err
	}

	inf.Start(ch)
	inf.WaitForCacheSync(ch)

	return informer, nil
}

func NewFakePDBInformer(ch chan struct{}, objects []runtime.Object) (PDBInformer, error) {
	return NewFakeInformer(ch, objects)
}

func NewFakePodInformer(ch chan struct{}, objects []runtime.Object) (PodInformer, error) {
	return NewFakeInformer(ch, objects)
}
