package filters

import (
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/groups"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
)

func NewDrainBufferFilter(drainBuffer drainbuffer.DrainBuffer, clock clock.Clock, groupKeyGetter groups.GroupKeyGetter) Filter {
	return FilterFromFunction(
		"drain_buffer",
		func(n *v1.Node) bool {
			groupKey := groupKeyGetter.GetGroupKey(n)
			return drainBuffer.NextDrain(groupKey).Before(clock.Now())
		},
	)
}
