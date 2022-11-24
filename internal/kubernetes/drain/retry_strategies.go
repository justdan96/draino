package drain

import (
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
)

type RetryStrategy interface {
	GetName() string
	GetDuration(retryCount int) time.Duration
	GetMaxRetries() int
}

// StaticRetryStrategy
type StaticRetryStrategy struct {
	MaxRetries int
	Delay      time.Duration
}

var _ RetryStrategy = &StaticRetryStrategy{}

func (_ *StaticRetryStrategy) GetName() string {
	return "StaticRetryStrategy"
}

func (strategy *StaticRetryStrategy) GetDuration(_ int) time.Duration {
	return strategy.Delay
}

func (strategy *StaticRetryStrategy) GetMaxRetries() int {
	return strategy.MaxRetries
}

// ExponentialRetryStrategy
type ExponentialRetryStrategy struct {
	Delay time.Duration
}

var _ RetryStrategy = &ExponentialRetryStrategy{}

func (_ *ExponentialRetryStrategy) GetName() string {
	return "ExponentialRetryStrategy"
}

func (strategy *ExponentialRetryStrategy) GetDuration(retryCount int) time.Duration {
	// The first retry should return 1 * delay, But 2 ^ 1 = 2, which means it would return 2 * delay
	// This means that we have to subtract one from the retryCount, so that 2 ^ 0 = 1 -> 1 * duration for retryCount = 1
	retries := retryCount - 1
	if retries < 0 {
		return 0
	}

	var exponent int64 = 2 ^ int64(retryCount)
	return time.Duration(exponent) * strategy.Delay
}

func (_ *ExponentialRetryStrategy) GetMaxRetries() int {
	return 10
}

// NodeAnnotationRetryStrategy
type NodeAnnotationRetryStrategy struct {
	MaxRetries      *int
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
		return
	}
	if !useDefault {
		useDefault = false
		maxRetries := int(attempts)
		nodeRetryStrat.MaxRetries = &maxRetries
	}

	if val, exist := node.Annotations[kubernetes.CustomRetryBackoffDelayAnnotation]; exist {
		durationValue, err := time.ParseDuration(val)
		if err != nil {
			useDefault = true
			funcErr = err
			return
		}

		useDefault = false
		nodeRetryStrat.Delay = &durationValue
	}

	strategy = nodeRetryStrat
	return
}

func (_ *NodeAnnotationRetryStrategy) GetName() string {
	return "CustomRetryStrategy"
}

func (strategy *NodeAnnotationRetryStrategy) GetDuration(retries int) time.Duration {
	if strategy.Delay == nil {
		return strategy.DefaultStrategy.GetDuration(retries)
	}
	return *strategy.Delay
}

func (strategy *NodeAnnotationRetryStrategy) GetMaxRetries() int {
	if strategy.MaxRetries == nil {
		return strategy.DefaultStrategy.GetMaxRetries()
	}
	return *strategy.MaxRetries
}
