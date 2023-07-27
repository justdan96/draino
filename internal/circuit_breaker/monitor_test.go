package circuitbreaker

import (
	"testing"
)

func Test_monitorBasedCircuitBreaker_isCorrectScoped(t *testing.T) {
	tests := []struct {
		name        string
		scopeTags   []string
		checkedTags []string
		want        bool
	}{
		{
			name:        "empty scope",
			scopeTags:   nil,
			checkedTags: []string{"whateverTag"},
			want:        true,
		},
		{
			name:        "empty tags",
			scopeTags:   []string{"whateverScope"},
			checkedTags: nil,
			want:        false,
		},
		{
			name:        "one/one match",
			scopeTags:   []string{"k1"},
			checkedTags: []string{"k1"},
			want:        true,
		},
		{
			name:        "3/3 match",
			scopeTags:   []string{"k1", "k2", "k3"},
			checkedTags: []string{"k1", "k2", "k3"},
			want:        true,
		},
		{
			name:        "scope subset",
			scopeTags:   []string{"k1"},
			checkedTags: []string{"k1", "k2"},
			want:        true,
		},
		{
			name:        "scope superset",
			scopeTags:   []string{"k1", "k2"},
			checkedTags: []string{"k1"},
			want:        false,
		},
		{
			name:        "OR1",
			scopeTags:   []string{"k1 OR k2"},
			checkedTags: []string{"k1"},
			want:        true,
		},
		{
			name:        "Real true",
			scopeTags:   []string{"kubernetes_cluster:goomy-a OR kube_cluster_name:goomy-a"},
			checkedTags: []string{"datacenter:us1.prod.dog", "kube_cluster_name:goomy-a", "something-else"},
			want:        true,
		},
		{
			name:        "Real true (order)",
			scopeTags:   []string{"kube_cluster_name:goomy-a OR kubernetes_cluster:goomy-a"},
			checkedTags: []string{"datacenter:us1.prod.dog", "kube_cluster_name:goomy-a", "something-else"},
			want:        true,
		},
		{
			name:        "Real false",
			scopeTags:   []string{"kubernetes_cluster:goomy-a OR kube_cluster_name:goomy-a"},
			checkedTags: []string{"datacenter:us1.prod.dog", "kube_cluster_name:goomy-b", "something-else"},
			want:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &monitorBasedCircuitBreaker{
				scopeTags: tt.scopeTags,
			}
			if got := m.isCorrectScoped(tt.checkedTags); got != tt.want {
				t.Errorf("isCorrectScoped() = %v, want %v", got, tt.want)
			}
		})
	}
}
