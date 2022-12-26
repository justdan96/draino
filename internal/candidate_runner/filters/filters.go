package filters

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"strings"
	"sync"
)

const (
	FilterSubSystem    = "filter"
	InputCount         = "input"
	FilteredOutCount   = "filtered_out"
	CapacityEfficiency = "cap_efficiency"
)

var (
	registerMetricOnce sync.Once

	inputCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: FilterSubSystem,
		Name:      InputCount,
		Help:      "Number of nodes entering the filter",
	}, []string{"filter", "group"})
	filteredOutCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: FilterSubSystem,
		Name:      FilteredOutCount,
		Help:      "Number of nodes filtered out by reason",
	}, []string{"filter", "group", "reason"})
	capEfficiencyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: FilterSubSystem,
		Name:      CapacityEfficiency,
		Help:      "Efficiency of capacity preset",
	}, []string{"filter", "group"})
)

// InitFilterMetrics initialize metrics for the generic filters
func InitFilterMetrics(Registry *prometheus.Registry) {
	registerMetricOnce.Do(func() {
		Registry.MustRegister(inputCountGauge)
		Registry.MustRegister(filteredOutCountGauge)
	})
}

type Filter interface {
	Name() string
	// Filter should be used to process and entire list. It is optimized for list processing
	Filter(nodes []*v1.Node) (keep []*v1.Node)
	// FilterNode returns more details for a given node. This is interesting for auditing and observability
	FilterNode(n *v1.Node) (keep bool, name, group, reason string)
}

type NodeFilterFunc func(n *v1.Node) bool
type NodeFilterFuncWithReason func(n *v1.Node) (bool, string)

type genericFilterFromFunc struct {
	name   string
	group  string
	logger logr.Logger
	f      NodeFilterFuncWithReason
}

func (g *genericFilterFromFunc) Name() string {
	return g.name
}

func (g *genericFilterFromFunc) FilterNode(n *v1.Node) (keep bool, name, group, reason string) {
	keep, reason = g.f(n)
	name = g.name
	group = g.group
	return
}

// keepProbabilityMap is used to manage memory allocation optimization
// we keep track of filtering ratio to better set the output slice capacity
var keepProbabilityMap = map[string]float64{}

func (g *genericFilterFromFunc) Filter(nodes []*v1.Node) (keep []*v1.Node) {
	keyRatio := g.name + "#" + g.group
	ratio, ok := keepProbabilityMap[keyRatio]
	if !ok {
		ratio = 0.1
	}

	keep = make([]*v1.Node, 0, int64(ratio*float64(len(nodes))+1)) // previous ratio plus a spare
	reasons := map[string]float64{}
	for _, n := range nodes {
		if ok, reason := g.f(n); ok {
			keep = append(keep, n)
			reasons[reason] += 1
		}
	}

	inputCountGauge.WithLabelValues(g.name, g.group).Set(float64(len(nodes)))
	for r, v := range reasons {
		filteredOutCountGauge.WithLabelValues(g.name, g.group, r).Set(v)
	}

	newRatio := float64(len(keep)) / float64(len(nodes))
	keepProbabilityMap[keyRatio] = newRatio
	capEfficiencyGauge.WithLabelValues(g.name, g.group).Set(float64(len(keep)))

	return
}

var _ Filter = &genericFilterFromFunc{}

func NodeFilterFuncFromInterfaceFunc(f func(o interface{}) bool) NodeFilterFunc {
	return func(n *v1.Node) bool {
		return f(n)
	}
}

func FilterFromFunction(name, group string, filterFunc NodeFilterFunc) Filter {
	return FilterFromFunctionWithReason(
		name,
		group,
		func(n *v1.Node) (bool, string) {
			return filterFunc(n), "rejected"
		},
	)
}

func FilterFromFunctionWithReason(name, group string, filterFunc NodeFilterFuncWithReason) Filter {
	return &genericFilterFromFunc{
		name:  name,
		group: group,
		f:     filterFunc,
	}
}

type CompositeFilter struct {
	logger  logr.Logger
	filters []Filter
}

const (
	CompositeFilterSeparator = "|"
)

func (c *CompositeFilter) Name() string {
	var names []string
	for _, f := range c.filters {
		names = append(names, f.Name())
	}
	return strings.Join(names, CompositeFilterSeparator)
}

func (c *CompositeFilter) Filter(nodes []*v1.Node) (keep []*v1.Node) {
	var filteringStr []string
	for _, f := range c.filters {
		nodes = f.Filter(nodes)
		filteringStr = append(filteringStr, fmt.Sprintf("%s:%d", f.Name(), len(nodes)))
		if len(nodes) == 0 {
			break
		}
	}
	c.logger.Info("filtering", "result", strings.Join(filteringStr, CompositeFilterSeparator))
	return nodes
}

func (c *CompositeFilter) FilterNode(n *v1.Node) (keep bool, name, group, reason string) {
	var filteringStr []string
	keep = true
	for _, f := range c.filters {
		k, _, g, r := f.FilterNode(n)
		if group != "" {
			if g != group {
				c.logger.Error(fmt.Errorf("composite filter on top of multiple groups"), "Bad filter configuration", "g1", g, "g2", group)
				return false, c.Name(), "ERROR", "multiple_group_filtering"
			}
		} else {
			group = g
		}
		keep = keep && k
		filteringStr = append(filteringStr, fmt.Sprintf("%v:%s", keep, r))
	}
	c.logger.Info("filtering", "result", filteringStr)
	return keep, c.Name(), group, strings.Join(filteringStr, CompositeFilterSeparator)
}

var _ Filter = &CompositeFilter{}
