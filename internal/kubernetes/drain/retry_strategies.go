package drain

import "time"

type RetryStrategy interface {
	GetName() string
	GetDuration(retryCount int) time.Duration
}

type StaticRetryStrategy struct {
	StaticDuration time.Duration
}

var _ RetryStrategy = &StaticRetryStrategy{}

func (_ *StaticRetryStrategy) GetName() string {
	return "StaticRetryStrategy"
}

func (strategy *StaticRetryStrategy) GetDuration(_ int) time.Duration {
	return strategy.StaticDuration
}
