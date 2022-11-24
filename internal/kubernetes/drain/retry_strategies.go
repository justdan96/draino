package drain

import "time"

type RetryStrategy interface {
	GetName() string
	GetDuration(retryCount int) time.Duration
}

// StaticRetryStrategy
type StaticRetryStrategy struct {
	Delay time.Duration
}

var _ RetryStrategy = &StaticRetryStrategy{}

func (_ *StaticRetryStrategy) GetName() string {
	return "StaticRetryStrategy"
}

func (strategy *StaticRetryStrategy) GetDuration(_ int) time.Duration {
	return strategy.Delay
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
