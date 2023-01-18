package kubernetes

import (
	"context"
	"fmt"
	"github.com/go-logr/zapr"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"reflect"
	"sort"
	"testing"

	openapi_v2 "github.com/google/gnostic/openapiv2"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	"k8s.io/client-go/rest"
)

type fakeDiscoveryInterface struct {
}

func (f fakeDiscoveryInterface) ServerGroups() (*metav1.APIGroupList, error) {
	return &metav1.APIGroupList{
		Groups: []metav1.APIGroup{
			{
				Name: "apps",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "apps/v1",
						Version:      "v1",
					},
					{
						GroupVersion: "apps/v1beta2",
						Version:      "v1beta2",
					},
				},
			},
			{
				Name: "",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "v1",
						Version:      "v1",
					},
				},
			},
			{
				Name: "datadoghq.com",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "datadoghq.com/v1",
						Version:      "v1",
					},
					{
						GroupVersion: "datadoghq.com/v1alpha1",
						Version:      "v1alpha1",
					},
				},
			},
		},
	}, nil
}

var (
	NodeV1Resource = metav1.APIResource{
		Name:         "nodes",
		SingularName: "node",
		Namespaced:   false,
		Group:        "",
		Version:      "v1",
		Kind:         "Node",
	}

	DaemonSetV1Apps = metav1.APIResource{
		Name:         "daemonsets",
		SingularName: "daemonset",
		Namespaced:   true,
		Group:        "apps",
		Version:      "v1",
		Kind:         "DaemonSet",
	}
	StatefulSetSetV1Apps = metav1.APIResource{
		Name:         "statefulsets",
		SingularName: "statefulset",
		Namespaced:   true,
		Group:        "apps",
		Version:      "v1",
		Kind:         "StatefulSet",
	}
	StatefulSetSetV1beta2Apps = metav1.APIResource{
		Name:         "statefulsets",
		SingularName: "statefulset",
		Namespaced:   true,
		Group:        "apps",
		Version:      "v1beta2",
		Kind:         "StatefulSet",
	}
	ExtendedDaemonSetV1alpha1Datadog = metav1.APIResource{
		Name:         "extendeddaemonsets",
		SingularName: "extendeddaemonset",
		Namespaced:   true,
		Group:        "datadoghq.com",
		Version:      "v1alpha1",
		Kind:         "ExtendedDaemonSet",
	}
	ExtendedDaemonSetReplicaV1alpha1Datadog = metav1.APIResource{
		Name:         "extendeddaemonsetreplicas",
		SingularName: "extendeddaemonsetreplica",
		Namespaced:   true,
		Group:        "datadoghq.com",
		Version:      "v1alpha1",
		Kind:         "ExtendedDaemonSetReplica",
	}
)

func (f fakeDiscoveryInterface) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {

	switch groupVersion {
	case "v1":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{NodeV1Resource},
		}, nil
	case "apps/v1":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{DaemonSetV1Apps, StatefulSetSetV1Apps},
		}, nil
	case "apps/v1beta2":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{StatefulSetSetV1beta2Apps},
		}, nil
	case "datadoghq.com/v1":
		return &metav1.APIResourceList{}, nil
	case "datadoghq.com/v1alpha1":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{ExtendedDaemonSetV1alpha1Datadog, ExtendedDaemonSetReplicaV1alpha1Datadog},
		}, nil
	}
	return nil, nil
}

func (f fakeDiscoveryInterface) RESTClient() rest.Interface {
	return nil // not needed
}

func (f fakeDiscoveryInterface) ServerResources() ([]*metav1.APIResourceList, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return nil, nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerVersion() (*version.Info, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) OpenAPISchema() (*openapi_v2.Document, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) OpenAPIV3() openapi.Client {
	return nil
}

func (f fakeDiscoveryInterface) WithLegacy() discovery.DiscoveryInterface {
	return nil
}

func newFakeDiscoveryClient() discovery.DiscoveryInterface {
	return &fakeDiscoveryInterface{}
}

func TestGetAPIResourcesForGVK(t *testing.T) {
	tests := []struct {
		name    string
		gvks    []string
		want    []*metav1.APIResource
		wantErr bool
	}{
		{
			name:    "empty case",
			gvks:    []string{""},
			want:    []*metav1.APIResource{nil},
			wantErr: false,
		},
		{
			name:    "nil case",
			want:    nil,
			wantErr: false,
		},
		{
			name:    "daemonset apps",
			gvks:    []string{"DaemonSet.apps"},
			want:    []*metav1.APIResource{&DaemonSetV1Apps},
			wantErr: false,
		},
		{
			name:    "daemonset apps v1",
			gvks:    []string{"DaemonSet.v1.apps"},
			want:    []*metav1.APIResource{&DaemonSetV1Apps},
			wantErr: false,
		},
		{
			name:    "statefulsets",
			gvks:    []string{"StatefulSet"},
			want:    []*metav1.APIResource{&StatefulSetSetV1Apps, &StatefulSetSetV1beta2Apps},
			wantErr: false,
		},
		{
			name:    "all",
			gvks:    []string{"StatefulSet", "DaemonSet", "", "ExtendedDaemonSet"},
			want:    []*metav1.APIResource{nil, &StatefulSetSetV1Apps, &StatefulSetSetV1beta2Apps, &DaemonSetV1Apps, &ExtendedDaemonSetV1alpha1Datadog},
			wantErr: false,
		},
		{
			name:    "error not found",
			gvks:    []string{"StatefulSet", "DaemonSet", "", "ExtendedDaemonSet", "Wrong"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetAPIResourcesForGVK(newFakeDiscoveryClient(), tt.gvks, zap.NewNop())
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAPIResourcesForGVK() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetAPIResourcesForGVK() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetAnnotationFromNodeAndThenPodOrController(t *testing.T) {
	testKey := "testKey"
	testValue := "testValue"
	nodeNoAnnotation := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}
	nodeNoKey := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{},
			Name:        "node1",
		},
	}
	nodeWithKey := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node1",
			Annotations: map[string]string{testKey: testValue},
		},
	}
	podNoAnnotation := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "no-annotation",
			Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{metav1.OwnerReference{
				Controller: &isController,
				Kind:       kindReplicaSet,
				Name:       deploymentName + "-xyz",
				APIVersion: "apps/v1",
			}},
		},
		Spec: core.PodSpec{
			NodeName: "node1",
		},
	}
	podNoKey := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "no-key",
			Namespace:   "ns",
			Annotations: map[string]string{},
			OwnerReferences: []metav1.OwnerReference{metav1.OwnerReference{
				Controller: &isController,
				Kind:       kindReplicaSet,
				Name:       deploymentName + "-xyz",
				APIVersion: "apps/v1",
			}},
		},
		Spec: core.PodSpec{
			NodeName: "node1",
		},
	}
	podWithKey := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "with-key",
			Namespace:   "ns",
			Annotations: map[string]string{testKey: testValue},
			OwnerReferences: []metav1.OwnerReference{metav1.OwnerReference{
				Controller: &isController,
				Kind:       kindReplicaSet,
				Name:       deploymentName + "-xyz",
				APIVersion: "apps/v1",
			}},
		},
		Spec: core.PodSpec{
			NodeName: "node1",
		},
	}
	DeploymentNoKey := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: "ns",
		},
	}
	DeploymentWithKey := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deploymentName,
			Namespace:   "ns",
			Annotations: map[string]string{testKey: testValue},
		},
	}

	pods := map[string]*core.Pod{
		podWithKey.Name:      podWithKey,
		podNoKey.Name:        podNoKey,
		podNoAnnotation.Name: podNoAnnotation,
	}

	tests := []struct {
		name    string
		node    *core.Node
		objects []runtime.Object
		want    AnnotationSearchResult[string]
		wantErr bool
	}{
		{
			name: "node no annotation",
			node: nodeNoAnnotation,
			want: AnnotationSearchResult[string]{
				Found: false,
			},
		},
		{
			name: "node no key",
			node: nodeNoKey,
			want: AnnotationSearchResult[string]{
				Found: false,
			},
		},
		{
			name: "node with key",
			node: nodeWithKey,
			want: AnnotationSearchResult[string]{
				Value: testValue,
				Found: true,
			},
		},
		{
			name: "node,pod no annotation, controller no key",
			node: nodeNoAnnotation,
			objects: []runtime.Object{
				podNoAnnotation, DeploymentNoKey,
			},
			want: AnnotationSearchResult[string]{
				Found: false,
			},
		},
		{
			name: "node,pod,controller no key",
			node: nodeNoKey,
			want: AnnotationSearchResult[string]{
				Found: false,
			},
			objects: []runtime.Object{
				podNoKey, DeploymentNoKey,
			},
		},
		{
			name: "node,pod,no key and controller with key",
			node: nodeNoKey,
			want: AnnotationSearchResult[string]{
				Found: true,
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{
					testValue: {{
						Value:        testValue,
						Pod:          podNoKey,
						OnController: true,
					}},
				},
			},
			objects: []runtime.Object{
				podNoKey, DeploymentWithKey, nodeNoKey,
			},
		},
		{
			name: "node no key, pod,controller with key",
			node: nodeNoKey,
			want: AnnotationSearchResult[string]{
				Found: true,
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{
					testValue: {{
						Value:        testValue,
						Pod:          podWithKey,
						OnController: false,
					}},
				},
			},
			objects: []runtime.Object{
				podWithKey, DeploymentWithKey,
			},
		},
	}
	testLogger := zapr.NewLogger(zap.NewNop())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: tt.objects})
			assert.NoError(t, err)

			fakeKubeClient := fakeclient.NewSimpleClientset(tt.objects...)
			store, closeFunc := RunStoreForTest(context.Background(), fakeKubeClient)
			defer closeFunc()

			fakeIndexer, err := index.New(wrapper.GetManagerClient(), wrapper.GetCache(), testLogger)
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			if err != nil {
				t.Fatalf("can't create fakeIndexer: %#v", err)
			}

			got, err := GetAnnotationFromNodeAndThenPodOrController(context.Background(), fakeIndexer, store, func(s string) (string, error) { return s, nil }, testKey, tt.node, true, true)
			if tt.wantErr != (err != nil) {
				fmt.Printf("%sGetAnnotationFromNodeAndThenPodOrController() ERR: %#v", tt.name, err)
				t.Failed()
			}
			if err != nil {
				return
			}

			// be sure that we are using the same pointer for comparison
			for _, v := range got.PodResults {
				for i := range v {
					v[i].Pod = pods[v[i].Pod.Name]
				}
			}
			for _, v := range tt.want.PodResults {
				for i := range v {
					v[i].Pod = pods[v[i].Pod.Name]
				}
			}

			assert.Equalf(t, tt.want, got, "GetAnnotationFromNodeAndThenPodOrController()")
		})
	}
}

func TestAnnotationSearchResult_IsValueUnique(t *testing.T) {
	tests := []struct {
		name string
		ar   AnnotationSearchResult[string]
		want bool
	}{
		{
			name: "empty",
			ar:   AnnotationSearchResult[string]{},
			want: false,
		},
		{
			name: "node level only",
			ar: AnnotationSearchResult[string]{
				Found: true,
				Value: "",
				Node:  &core.Node{},
			},
			want: true,
		},
		{
			name: "pod only and unique",
			ar: AnnotationSearchResult[string]{
				Found:      true,
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{"": {{Value: ""}}},
			},
			want: true,
		},
		{
			name: "pod only not unique",
			ar: AnnotationSearchResult[string]{
				Found:      true,
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{"": {{Value: ""}}, "a": {{Value: "a"}}},
			},
			want: false,
		},
		{
			name: "pod and node unique",
			ar: AnnotationSearchResult[string]{
				Found:      true,
				Value:      "a",
				Node:       &core.Node{},
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{"a": {{Value: "a"}}},
			},
			want: true,
		},
		{
			name: "pod and node not unique",
			ar: AnnotationSearchResult[string]{
				Found:      true,
				Value:      "a",
				Node:       &core.Node{},
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{"b": {{Value: "b"}}},
			},
			want: false,
		},
		{
			name: "pod and node not unique 2",
			ar: AnnotationSearchResult[string]{
				Found:      true,
				Value:      "a",
				Node:       &core.Node{},
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{"a": {{Value: "a"}}, "b": {{Value: "b"}}},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.ar.IsValueUnique(), "IsValueUnique()")
		})
	}
}

func TestAnnotationSearchResult_AsSlice(t *testing.T) {
	tests := []struct {
		name    string
		ar      AnnotationSearchResult[string]
		wantOut []string
	}{
		{
			name: "dupe node pod",
			ar: AnnotationSearchResult[string]{
				Found:      true,
				Value:      "a",
				Node:       &core.Node{},
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{"a": {{Value: "a"}}, "b": {{Value: "b"}}},
			},
			wantOut: []string{"a", "b"},
		},
		{
			name: "pod only",
			ar: AnnotationSearchResult[string]{
				Found:      true,
				PodResults: map[string][]AnnotationSearchResultOnPod[string]{"a": {{Value: "a"}}, "b": {{Value: "b"}}, "c": {{Value: "c"}}},
			},
			wantOut: []string{"a", "b", "c"},
		},
		{
			name: "nil",
			ar: AnnotationSearchResult[string]{
				Found: false,
			},
			wantOut: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sort.Strings(tt.wantOut)
			sort.Strings(tt.ar.AsSlice())
			assert.Equalf(t, tt.wantOut, tt.ar.AsSlice(), "AsSlice()")
		})
	}
}
