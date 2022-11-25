package drain

import (
	"math"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
)

type RetryStrategy interface {
	// GetName returns a unique name of the strategy
	GetName() string
	// GetDelay will return a delay based on the given retry count
	GetDelay(retryCount int) time.Duration
	// GetAlertThreashold will return the amount of accepted retries
	GetAlertThreashold() int
}

// StaticRetryStrategy is a very simple strategy, which always returns the same delay
type StaticRetryStrategy struct {
	AlertThreashold int
	Delay           time.Duration
}

var _ RetryStrategy = &StaticRetryStrategy{}

func (_ *StaticRetryStrategy) GetName() string {
	return "StaticRetryStrategy"
}

func (strategy *StaticRetryStrategy) GetDelay(_ int) time.Duration {
	return strategy.Delay
}

func (strategy *StaticRetryStrategy) GetAlertThreashold() int {
	return strategy.AlertThreashold
}

// ExponentialRetryStrategy is using the exponential backoff algorithm
// retry 0 -> 0 delay
// retry 1 -> 1 delay
// retry 2 -> 2 delay
// Retry 3 -> 4 delay
type ExponentialRetryStrategy struct {
	AlertThreashold int
	Delay           time.Duration
}

var _ RetryStrategy = &ExponentialRetryStrategy{}

func (_ *ExponentialRetryStrategy) GetName() string {
	return "ExponentialRetryStrategy"
}

func (strategy *ExponentialRetryStrategy) GetDelay(retryCount int) time.Duration {
	// The first retry should return 1 * delay, But 2 ^ 1 = 2, which means it would return 2 * delay
	// This means that we have to subtract one from the retryCount, so that 2 ^ 0 = 1 -> 1 * duration for retryCount = 1
	retries := retryCount - 1
	if retries < 0 {
		return 0
	}

	exponent := int64(math.Pow(2, float64(retries)))
	return time.Duration(exponent) * strategy.Delay
}

func (strategy *ExponentialRetryStrategy) GetAlertThreashold() int {
	return strategy.AlertThreashold
}

// NodeAnnotationRetryStrategy is parsing specific node annotations and using their values to take delay decisions.
// It will return useDefault=true if none of the annotations was set.
// If only one annotation is set it will use the given default strategy as fallback for the others
type NodeAnnotationRetryStrategy struct {
	AlertThreashold *int
	Delay           *time.Duration
	DefaultStrategy RetryStrategy
}

var _ RetryStrategy = &NodeAnnotationRetryStrategy{}

func BuildNodeAnnotationRetryStrategy(node *v1.Node, defaultStrategy RetryStrategy) (strategy RetryStrategy, useDefault bool, funcErr error) {
	funcErr = nil
	useDefault = true
	nodeRetryStrat := &NodeAnnotationRetryStrategy{DefaultStrategy: defaultStrategy}

	attempts, useDefault, err := kubernetes.GetNodeRetryMaxAttempt(node)
	if err != nil {
		funcErr = err
	} else if !useDefault {
		useDefault = false
		alertThreashold := int(attempts)
		nodeRetryStrat.AlertThreashold = &alertThreashold
	}

	if val, exist := node.Annotations[kubernetes.CustomRetryBackoffDelayAnnotation]; exist {
		durationValue, err := time.ParseDuration(val)
		if err != nil {
			funcErr = err
		} else {
			useDefault = false
			nodeRetryStrat.Delay = &durationValue
		}
	}

	strategy = nodeRetryStrat
	return
}

func (_ *NodeAnnotationRetryStrategy) GetName() string {
	return "NodeAnnotationRetryStrategy"
}

func (strategy *NodeAnnotationRetryStrategy) GetDelay(retries int) time.Duration {
	if strategy.Delay == nil {
		return strategy.DefaultStrategy.GetDelay(retries)
	}
	return *strategy.Delay
}

func (strategy *NodeAnnotationRetryStrategy) GetAlertThreashold() int {
	if strategy.AlertThreashold == nil {
		return strategy.DefaultStrategy.GetAlertThreashold()
	}
	return *strategy.AlertThreashold
}
