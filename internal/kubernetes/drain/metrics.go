package drain

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	RetryWallSubsystem = "retry_wall"

	TagNodeName = "node_name"
)

var (
	registerMetricsOnce sync.Once

	nodeRetries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: RetryWallSubsystem,
		Name:      "node_retries",
		Help:      "Number of retries for each node",
	}, []string{TagNodeName})
)

func RegisterMetrics(registry *prometheus.Registry) {
	registerMetricsOnce.Do(func() {
		registry.MustRegister(nodeRetries)
	})
}
