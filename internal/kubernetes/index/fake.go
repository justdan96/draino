package index

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

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

func NewFakeIndexer(ch chan struct{}, objects []runtime.Object) (*Indexer, error) {
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

func NewFakePDBIndexer(ch chan struct{}, objects []runtime.Object) (PDBIndexer, error) {
	return NewFakeIndexer(ch, objects)
}

func NewFakePodIndexer(ch chan struct{}, objects []runtime.Object) (PodIndexer, error) {
	return NewFakeIndexer(ch, objects)
}

func createPod(name, ns, nodeName string, isReady bool, ls ...labels.Set) *corev1.Pod {
	var label labels.Set = map[string]string{}
	if len(ls) > 0 {
		label = ls[0]
	}
	ready := corev1.ConditionFalse
	if isReady {
		ready = corev1.ConditionTrue
	}
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    label,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.ContainersReady,
					Status: ready,
				},
			},
		},
	}
}
