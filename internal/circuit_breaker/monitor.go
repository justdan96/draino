package circuitbreaker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/DataDog/compute-go/ddclient"
	"github.com/DataDog/compute-go/logs"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/flowcontrol"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	DDClientConfigFile = "/etc/config/datadog-client.yaml"
)

type monitorBasedCircuitBreaker struct {
	name         string
	period       time.Duration
	monitorTag   string
	scopeTags    []string
	limiter      flowcontrol.RateLimiter
	logger       logr.Logger
	defaultState State

	currentState State
	ddClient     *ddclient.Client
}

// implement 2 interfaces
var _ manager.Runnable = &monitorBasedCircuitBreaker{}
var _ NamedCircuitBreaker = &monitorBasedCircuitBreaker{}

func (m *monitorBasedCircuitBreaker) Name() string {
	return m.name
}

func (m *monitorBasedCircuitBreaker) State() State {
	if m.currentState == "" {
		return m.defaultState
	}
	return m.currentState
}

func (m *monitorBasedCircuitBreaker) IsOpen() bool {
	return m.State() == Open
}

func (m *monitorBasedCircuitBreaker) IsHalfOpen() bool {
	return m.State() == HalfOpen
}

func (m *monitorBasedCircuitBreaker) IsClose() bool {
	return m.State() == Closed
}

func (m *monitorBasedCircuitBreaker) HalfOpenTry() bool {
	return m.limiter.TryAccept()
}

func NewMonitorBasedCircuitBreaker(name string, logger logr.Logger, period time.Duration, monitorTags []string, scopeTags []string, limiter flowcontrol.RateLimiter, defaultState State) (*monitorBasedCircuitBreaker, error) {
	if defaultState == "" {
		defaultState = Open
	}
	allTags := append([]string{"draino_circuit_breaker"}, monitorTags...)
	for i := 0; i < len(allTags); i++ {
		allTags[i] = "tag:" + allTags[i]
	}

	monitorCircuitBreaker := &monitorBasedCircuitBreaker{
		name:         name,
		period:       period,
		monitorTag:   strings.Join(allTags, " "),
		scopeTags:    scopeTags,
		limiter:      limiter,
		logger:       logger.WithName("CircuitBreaker-" + name),
		defaultState: defaultState,
	}

	ddcfg, err := ddclient.GetClientConfig(DDClientConfigFile)
	if err != nil {
		return nil, err
	}
	logger.Info("Creating DDClient", "config", ddcfg.ObfuscatedCopy())
	if monitorCircuitBreaker.ddClient, err = ddclient.NewClient(context.Background(), ddcfg); err != nil {
		return nil, err
	}
	return monitorCircuitBreaker, nil
}

// Start run the Circuit breaker to the context is Done, blocking call
func (m *monitorBasedCircuitBreaker) Start(ctx context.Context) error {
	m.logger.Info("starting")
	generateMetric(m)
	wait.Until(func() { m.runCircuitBreaker() }, m.period, ctx.Done())
	return nil
}

func (m *monitorBasedCircuitBreaker) runCircuitBreaker() {
	monitorClient := datadogV1.NewMonitorsApi(m.ddClient.APIClient)

	monitorGroupQuery := datadogV1.NewSearchMonitorGroupsOptionalParameters().WithQuery(m.monitorTag)
	grpResponse, httpResp, err := monitorClient.SearchMonitorGroups(m.ddClient.Context, *monitorGroupQuery)
	if err != nil {
		m.logger.Error(err, "failed to SearchMonitorGroups")
	}
	if httpResp.StatusCode != http.StatusOK {
		msg, err := io.ReadAll(httpResp.Body)
		if err != nil {
			msg = []byte(err.Error())
		}
		// close response body
		httpResp.Body.Close()
		m.logger.Error(fmt.Errorf("failed to SearchMonitorGroups - http error"), string(msg))
		return
	}

	if len(grpResponse.Groups) == 0 {
		m.logger.Error(fmt.Errorf("no monitor found for circuit breaker (%s)", m.monitorTag), "Circuit Breaker will be left in default state", "default_state", m.defaultState)
	}
	m.logger.V(logs.ZapDebug).Info("Count of monitor groups found", "count", len(grpResponse.Groups))

	var oneWarning bool
	for _, grp := range grpResponse.Groups {
		if !m.isCorrectScoped(grp.GroupTags) {
			continue
		}
		status := grp.GetStatus()
		if status != datadogV1.MONITOROVERALLSTATES_OK {
			if status == datadogV1.MONITOROVERALLSTATES_WARN {
				oneWarning = true
				continue
			}
			m.setState(Open)
			return
		}
	}
	if oneWarning {
		m.setState(HalfOpen)
		return
	}
	m.setState(Closed)
}

func (m *monitorBasedCircuitBreaker) isCorrectScoped(checkedTags []string) bool {
	checkedTagSet := make(map[string]struct{}, len(checkedTags))
	for _, k := range checkedTags {
		checkedTagSet[k] = struct{}{}
	}
	for _, tag := range m.scopeTags {
		var orMatch bool
		for _, scopeTag := range strings.Split(tag, " OR ") {
			if _, orMatch = checkedTagSet[scopeTag]; orMatch {
				break
			}
		}
		if !orMatch {
			return false
		}
	}
	return true
}

func (m *monitorBasedCircuitBreaker) setState(state State) {
	if m.currentState != state {
		m.logger.Info("State change", "state", state)
		generateMetric(m)
		m.currentState = state
	}
}
