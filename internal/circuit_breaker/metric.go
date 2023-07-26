package circuitbreaker

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	circuitBreaker = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "CircuitBreaker",
		Name:      "CircuitBreaker",
		Help:      "current state of the circuit breaker",
	}, []string{"name"})
)

const (
	openGaugeValue     = 1.0
	halfOpenGaugeValue = 0.5
	closedGaugeValue   = 0
)

func InitMetrics(Registry *prometheus.Registry) {
	Registry.MustRegister(circuitBreaker)
}

func generateMetric(cb NamedCircuitBreaker) {
	g := circuitBreaker.WithLabelValues(cb.Name())
	switch cb.State() {
	case Open:
		g.Set(openGaugeValue)
	case HalfOpen:
		g.Set(halfOpenGaugeValue)
	case Closed:
		g.Set(closedGaugeValue)
	}
}
