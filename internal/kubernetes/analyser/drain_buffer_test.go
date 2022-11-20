package analyser

import (
	"context"
	"github.com/go-logr/zapr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

func Test_getLatestDisruption(t *testing.T) {
	date14 := time.Date(2022, 11, 18, 14, 0, 0, 0, time.UTC)
	date15 := time.Date(2022, 11, 18, 15, 0, 0, 0, time.UTC)
	date16 := time.Date(2022, 11, 18, 16, 0, 0, 0, time.UTC)

	pdbBlocked := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbblocked"},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionFalse,
					LastTransitionTime: meta.Time{date14},
				},
			},
		},
	}
	pdbOk := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbok"},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionTrue,
					LastTransitionTime: meta.Time{date15},
				},
			},
		},
	}
	pdbOk2 := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbok2"},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionTrue,
					LastTransitionTime: meta.Time{date16},
				},
			},
		},
	}
	pdbNoCondition := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbNoConditions", CreationTimestamp: meta.Time{date16}},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{},
		},
	}
	pdbNoTransitionTime := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbNoTransitionTime", CreationTimestamp: meta.Time{date16}},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionTrue,
					LastTransitionTime: meta.Time{},
				},
			},
		},
	}
	pdbNoTransitionTimeButBlocked := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbNoTransitionTime", CreationTimestamp: meta.Time{date16}},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionFalse,
					LastTransitionTime: meta.Time{},
				},
			},
		},
	}
	testLogger := zapr.NewLogger(zap.NewNop())
	tests := []struct {
		name                  string
		pdbsForPods           []*policyv1.PodDisruptionBudget
		wantDisruptionAllowed bool
		wantStableSince       time.Time
		wantDisruptedPDB      *policyv1.PodDisruptionBudget
	}{
		{
			name:                  "nil",
			pdbsForPods:           nil,
			wantDisruptionAllowed: true,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      nil,
		},
		{
			name:                  "no pdb",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{},
			wantDisruptionAllowed: true,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      nil,
		},
		{
			name:                  "2 pdbs with no disruption allowed",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbBlocked, pdbOk},
			wantDisruptionAllowed: false,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      pdbBlocked,
		},
		{
			name:                  "2 pdbs with disruption allowed",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbOk2, pdbOk},
			wantDisruptionAllowed: true,
			wantStableSince:       date16,
			wantDisruptedPDB:      pdbOk2,
		},
		{
			name:                  "no conditions",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbNoCondition, pdbOk},
			wantDisruptionAllowed: true,
			wantStableSince:       date16,
			wantDisruptedPDB:      pdbNoCondition,
		},
		{
			name:                  "no transitionTime",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbNoTransitionTime, pdbOk},
			wantDisruptionAllowed: true,
			wantStableSince:       date16,
			wantDisruptedPDB:      pdbNoTransitionTime,
		},
		{
			name:                  "no transitionTime and blocked",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbNoTransitionTimeButBlocked, pdbOk},
			wantDisruptionAllowed: false,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      pdbNoTransitionTimeButBlocked,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDisruptionAllowed, gotStableSince, gotDisruptedPDB := getLatestDisruption(testLogger, tt.pdbsForPods)
			assert.Equalf(t, tt.wantDisruptionAllowed, gotDisruptionAllowed, "disruptionAllowed")
			assert.Equalf(t, tt.wantStableSince, gotStableSince, "stableSince")
			assert.Equalf(t, tt.wantDisruptedPDB, gotDisruptedPDB, "pdb")
		})
	}
}

func Test_drainBufferChecker_DrainBufferAcceptsDrain(t *testing.T) {
	now := time.Date(2022, 11, 19, 18, 00, 00, 00, time.UTC)
	drainBufferOneHour := time.Hour
	tests := []struct {
		name               string
		objects            []runtime.Object
		node               *corev1.Node
		defaultDrainBuffer *time.Duration
		want               bool
	}{
		{
			name:    "just node, can drain",
			objects: nil,
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name: "new pdb, using pdb creation timestamp",
			objects: []runtime.Object{&corev1.Pod{
				ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
					Labels: map[string]string{"app": "test"}},
				Spec: corev1.PodSpec{NodeName: "node1"},
			},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns", CreationTimestamp: meta.Time{Time: now}},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: false,
		},
		{
			name:               "new pdb, using pdb creation timestamp old enough to accept disruption",
			defaultDrainBuffer: &drainBufferOneHour,
			objects: []runtime.Object{&corev1.Pod{
				ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
					Labels: map[string]string{"app": "test"}},
				Spec: corev1.PodSpec{NodeName: "node1"},
			},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns", CreationTimestamp: meta.Time{Time: now.Add(-2 * drainBufferOneHour)}},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name:               "pdb not allowing disruption for long enough",
			defaultDrainBuffer: &drainBufferOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
						Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{now.Add(-drainBufferOneHour).Add(time.Minute)}, // missing one minute to allow
							},
						},
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: false,
		},
		{
			name:               "pdb allowing disruption +1 minute after drainBuffer",
			defaultDrainBuffer: &drainBufferOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
						Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&corev1.Pod{ // unrelated pod (just to create noise)
					ObjectMeta: meta.ObjectMeta{Name: "podx", Namespace: "ns",
						Labels: map[string]string{"app": "x"}},
					Spec: corev1.PodSpec{NodeName: "nodex"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{now.Add(-drainBufferOneHour).Add(-time.Minute)}, // one extra minute: allowed
							},
						},
					},
				},
				&policyv1.PodDisruptionBudget{ // unrelated PDB just to create noise
					ObjectMeta: meta.ObjectMeta{Name: "pdbx", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "x"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{now.Add(-drainBufferOneHour).Add(+time.Minute)}, // unrelated pod would block another node
							},
						},
					},
				},
				&corev1.Node{ // unrelated node just to create noise
					ObjectMeta: meta.ObjectMeta{
						Name: "nodex",
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name:               "one of the 2 pods of node1 is blocking",
			defaultDrainBuffer: &drainBufferOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
						Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&corev1.Pod{ // unrelated pod (just to create noise)
					ObjectMeta: meta.ObjectMeta{Name: "pod2", Namespace: "ns",
						Labels: map[string]string{"app": "2"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{now.Add(-drainBufferOneHour).Add(-time.Minute)}, // one extra minute: allowed
							},
						},
					},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb2", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "2"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{now.Add(-drainBufferOneHour).Add(+time.Minute)}, // unrelated pod would block another node
							},
						},
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: false,
		},
	}

	testLogger := zapr.NewLogger(zap.NewNop())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan struct{})
			defer close(ch)
			tt.objects = append(tt.objects, tt.node)

			fakeClient := fake.NewClientBuilder().WithRuntimeObjects(tt.objects...).Build()
			fakeKubeClient := fakeclient.NewSimpleClientset(tt.objects...)
			er := kubernetes.NewEventRecorder(record.NewFakeRecorder(1000))
			store, closeFunc := kubernetes.RunStoreForTest(context.Background(), fakeKubeClient)
			defer closeFunc()
			fakeIndexer, err := index.NewFakeIndexer(ch, tt.objects)
			if err != nil {
				t.Fatalf("can't create fakeIndexer: %#v", err)
			}
			d := NewDrainBufferChecker(context.Background(), testLogger, fakeClient, er, store, *fakeIndexer,
				DrainBufferCheckerConfiguration{
					DefaultDrainBuffer: tt.defaultDrainBuffer,
				})
			assert.Equalf(t, tt.want, d.DrainBufferAcceptsDrain(context.Background(), tt.node, now), "DrainBufferAcceptsDrain bad result")
		})
	}
}
