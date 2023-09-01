package kubernetes

import (
	"reflect"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOffendingConditions(t *testing.T) {
	cases := []struct {
		name       string
		obj        *core.Node
		conditions []string
		expected   []SuppliedCondition
	}{
		{
			name: "SingleMatchingCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool"},
			expected:   []SuppliedCondition{{DisplayName: "Cool", Type: "Cool", Status: core.ConditionTrue, parsedExpectedResolutionTime: DefaultExpectedResolutionTime}},
		},
		{
			name: "ManyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
					{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{DisplayName: "Cool", Type: "Cool", Status: core.ConditionTrue, parsedExpectedResolutionTime: DefaultExpectedResolutionTime},
				{DisplayName: "Rad", Type: "Rad", Status: core.ConditionTrue, parsedExpectedResolutionTime: DefaultExpectedResolutionTime},
			},
		},
		{
			name: "PartiallyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
					{Type: "Rad", Status: core.ConditionFalse},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{DisplayName: "Cool", Type: "Cool", Status: core.ConditionTrue, parsedExpectedResolutionTime: DefaultExpectedResolutionTime},
			},
		},
		{
			name: "PartiallyAbsentConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{DisplayName: "Rad", Type: "Rad", Status: core.ConditionTrue, parsedExpectedResolutionTime: DefaultExpectedResolutionTime},
			},
		},
		{
			name: "SingleFalseCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			conditions: []string{"Cool"},
			expected:   nil,
		},
		{
			name:       "NoNodeConditions",
			obj:        &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			conditions: []string{"Cool"},
			expected:   nil,
		},
		{
			name: "NoFilterConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			expected: nil,
		},
		{
			name: "NewConditionFormat",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown},
				}},
			},
			conditions: []string{`Cool-display={"type": "Cool","conditionStatus":"Unknown", "delay":"10m"}`},
			expected: []SuppliedCondition{
				{DisplayName: "Cool-display", Type: "Cool", Status: core.ConditionUnknown, parsedDelay: 10 * time.Minute, Delay: "10m", parsedExpectedResolutionTime: DefaultExpectedResolutionTime},
			},
		},
		{
			name: "NewConditionFormatDurationNotEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-9) * time.Minute))},
				}},
			},
			conditions: []string{`Cool={"conditionStatus":"Unknown", "delay":"10m", "Priority":55}`},
			expected:   nil,
		},
		{
			name: "NewConditionFormatDurationIsEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-15) * time.Minute))},
				}},
			},
			conditions: []string{`Cool={"conditionStatus":"Unknown", "delay":"14m","priority":99}`},
			expected: []SuppliedCondition{
				{DisplayName: "Cool", Type: "Cool", Status: core.ConditionUnknown, parsedDelay: 14 * time.Minute, Delay: "14m", Priority: 99, parsedExpectedResolutionTime: DefaultExpectedResolutionTime},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			suppliedConditions, err := ParseConditions(tc.conditions)
			if err != nil {
				t.Errorf(err.Error())
				return
			}

			badConditions := GetNodeOffendingConditions(tc.obj, suppliedConditions)
			if !reflect.DeepEqual(badConditions, tc.expected) {
				t.Errorf("offendingConditions(tc.obj): want %#v, got %#v", tc.expected, badConditions)
			}
		})
	}
}
